# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from __future__ import annotations

from typing import Any, Awaitable, Callable, NamedTuple, Optional
import asyncio
import json
import logging
import time

from aiohttp import web
from aiohttp.http import WSCloseCode, WSMessage, WSMsgType

from .errors import WebsocketClosedError, WebsocketErrorResponse

Data = dict[str, Any]
CommandHandler = Callable[["WebsocketHandler", Data], Awaitable[Optional[Data]]]


class RequestWaiter(NamedTuple):
    fut: asyncio.Future
    command: str

    def redact_log_content(self, data: str, req: dict) -> str:
        if self.command in SENSITIVE_RESPONSES:
            content = req.get("data")
            if isinstance(content, (list, dict)):
                return f"content omitted: {type(content).__name__} with {len(content)} items"
            return "content omitted"
        elif len(data) > LOG_CONTENT_MAX_LENGTH:
            return data[:LOG_CONTENT_MAX_LENGTH] + "..."
        else:
            return str(req)


SENSITIVE_REQUESTS = ("start_dm", "resolve_identifier", "start_sync", "push_key")
SENSITIVE_RESPONSES = ("list_contacts", "start_dm", "resolve_identifier")
LOG_CONTENT_MAX_LENGTH = 4096


class WebsocketHandler:
    _ws: web.WebSocketResponse
    log: logging.Logger
    type_name: str
    _request_waiters: dict[int, RequestWaiter]
    _command_handlers: dict[str, CommandHandler]
    _prev_req_id: int
    proto: int
    timeouts: int
    queue_task: asyncio.Task | None
    last_received: float
    dead: bool
    identifier: str | None

    def __init__(
        self,
        type_name: str,
        log: logging.Logger,
        proto: str,
        version: int,
        identifier: str | None = None,
        heartbeat: float | None = None,
    ) -> None:
        self.type_name = type_name
        self._ws = web.WebSocketResponse(protocols=(proto,), heartbeat=heartbeat)
        self.log = log
        self.proto = version
        self.timeouts = 0
        self._prev_req_id = 0
        self._request_waiters = {}
        self._command_handlers = {}
        self.queue_task = None
        self.last_received = 0.0
        self.dead = False
        self.identifier = identifier

    @property
    def response(self) -> web.WebSocketResponse:
        return self._ws

    def set_handler(self, command: str, handler: CommandHandler) -> None:
        self._command_handlers[command] = handler

    async def _call_handler(
        self, handler: CommandHandler, command: str, req_id: int | None, data: Data
    ) -> None:
        try:
            resp = await handler(self, data)
        except Exception as e:
            self.log.exception(f"Error handling {command} {req_id or '<no id>'}")
            if req_id is not None:
                await self.send(
                    command="error", id=req_id, data={"code": type(e).__name__, "message": str(e)}
                )
        else:
            if req_id is not None:
                await self.send(command="response", id=req_id, data=resp)

    def _clear_request_waiters(self) -> None:
        for req_id, waiter in self._request_waiters.items():
            if not waiter.fut.done():
                waiter.fut.set_exception(WebsocketClosedError())
        self._request_waiters = {}

    def _handle_text(self, msg: WSMessage) -> None:
        try:
            req = msg.json()
        except json.JSONDecodeError:
            self.log.debug(f"Non-JSON data received: {msg.data}")
            return
        data = req.get("data")
        try:
            command = req["command"]
        except (KeyError, TypeError):
            self.log.debug(f"Unhandled data received: {req}")
            return
        try:
            req_id = int(req["id"])
        except (KeyError, ValueError):
            req_id = None

        if command in ("response", "error"):
            if req_id is None:
                self.log.debug(f"Unhandled response received: {req}")
                return
            try:
                waiter = self._request_waiters.pop(req_id)
            except KeyError:
                self.log.debug(f"Unhandled response received: {req}")
            else:
                log_content = waiter.redact_log_content(msg.data, req)
                if waiter.fut.cancelled():
                    self.log.debug(
                        f"Got response to {req_id} ({waiter.command}), "
                        f"but the waiter is cancelled: {log_content}"
                    )
                    return
                self.log.debug(f"Received response to {req_id} ({waiter.command}): {log_content}")
                if command == "response":
                    waiter.fut.set_result(data)
                elif command == "error":
                    waiter.fut.set_exception(WebsocketErrorResponse(data))
            return

        try:
            handler = self._command_handlers[command]
        except KeyError:
            self.log.debug(f"Unhandled request received: {req}")
            if req_id:
                resp = {"code": "UnknownCommand", "message": f"Unknown command {command}"}
                asyncio.create_task(self.send(command="error", id=req_id, data=resp))
        else:
            log_data = data if command not in SENSITIVE_REQUESTS else "content omitted"
            self.log.debug(f"Received {command} {req_id or '<no id>'}: {log_data}")
            asyncio.create_task(self._call_handler(handler, command, req_id, data))

    async def close(self, code: int | WSCloseCode, status: str | None = None) -> None:
        try:
            message = f"Closing websocket ({code} / {status})"
            self.log.debug(message)
            res_message = (
                json.dumps({"command": "disconnect", "status": status}).encode("utf-8")
                if status
                else b""
            )
            ret = await self._ws.close(code=code, message=res_message)
            self.log.debug(f"Websocket closed: {ret}")
            self.dead = True
        except Exception:
            self.log.exception("Error sending close to client")

    async def send(self, raise_errors: bool = False, **kwargs: Any) -> None:
        try:
            await self._ws.send_json(kwargs)
        except Exception:
            self.log.exception("Error sending data to client")
            if raise_errors:
                raise

    async def request(
        self,
        command: str,
        *,
        top_level_data: dict[str, Any] | None = None,
        raise_errors: bool = False,
        **kwargs: Any,
    ) -> Data:
        self._prev_req_id += 1
        req_id = self._prev_req_id
        fut = asyncio.get_running_loop().create_future()
        self._request_waiters[req_id] = RequestWaiter(fut=fut, command=command)
        await self.send(
            command=command,
            id=req_id,
            raise_errors=raise_errors,
            data=kwargs,
            **(top_level_data or {}),
        )
        self.log.debug(f"Sent request {req_id} ({command})")
        return await fut

    def prepare(self, req: web.Request) -> Awaitable[Any]:
        return self._ws.prepare(req)

    async def handle(self) -> None:
        try:
            self.log.debug(f"{self.type_name} opened (proto: {self.proto})")
            msg: WSMessage
            async for msg in self._ws:
                self.last_received = time.time()
                if msg.type == WSMsgType.ERROR:
                    self.log.error("Error in websocket connection", exc_info=self._ws.exception())
                    break
                elif msg.type == WSMsgType.CLOSE:
                    self.log.debug("Websocket close message received")
                    break
                elif msg.type == WSMsgType.TEXT:
                    if self.dead:
                        self.log.warning("Received data even though websocket is marked as dead")
                        if self._ws._writer:
                            self._ws._writer.transport.abort()
                        continue
                    try:
                        self._handle_text(msg)
                    except Exception:
                        self.log.exception("Error handling websocket text message")
                else:
                    self.log.debug(
                        "Unhandled websocket message of type %s: %s", msg.type, msg.data
                    )
        except Exception:
            self.log.exception("Fatal error in websocket handler")
        else:
            self.log.debug(f"{self.type_name} closed")
        self._clear_request_waiters()
