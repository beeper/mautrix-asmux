# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional
import asyncio
import json
import logging
import time

from aiohttp import web
from aiohttp.http import WSCloseCode, WSMessage, WSMsgType

from .errors import WebsocketErrorResponse

Data = dict[str, Any]
CommandHandler = Callable[["WebsocketHandler", Data], Awaitable[Optional[Data]]]


class WebsocketHandler:
    _ws: web.WebSocketResponse
    log: logging.Logger
    type_name: str
    _request_waiters: dict[int, asyncio.Future]
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
    ) -> None:
        self.type_name = type_name
        self._ws = web.WebSocketResponse(protocols=(proto,))
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
            if not waiter.done():
                waiter.set_exception(WebsocketClosedError())
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
                if waiter.cancelled():
                    self.log.debug(f"Got response to {req_id}, but the waiter is cancelled: {req}")
                    return
                self.log.debug(f"Received response to {req_id}: {req}")
                if command == "response":
                    waiter.set_result(data)
                elif command == "error":
                    waiter.set_exception(WebsocketErrorResponse(data))
            return

        try:
            handler = self._command_handlers[command]
        except KeyError:
            self.log.debug(f"Unhandled request received: {req}")
            if req_id:
                resp = {"code": "UnknownCommand", "message": f"Unknown command {command}"}
                asyncio.create_task(self.send(command="error", id=req_id, data=resp))
        else:
            self.log.debug(f"Received {command} {req_id or '<no id>'}: {data}")
            asyncio.create_task(self._call_handler(handler, command, req_id, data))

    def cancel_queue_task(self, reason: str) -> None:
        if self.queue_task is not None and not self.queue_task.done():
            self.queue_task.cancel(reason)
            self.log.debug("Cancelled queue task (%s)", reason)
        else:
            self.log.debug("Queue task seems to be cancelled already (%s)", reason)

    async def close(self, code: int | WSCloseCode, status: str | None = None) -> None:
        try:
            message = f"Closing websocket ({code} / {status})"
            self.log.debug(message)
            self.cancel_queue_task(message)
            message = (
                json.dumps({"command": "disconnect", "status": status}).encode("utf-8")
                if status
                else None
            )
            ret = await self._ws.close(code=code, message=message)
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
    ) -> Optional[Data]:
        self._prev_req_id += 1
        req_id = self._prev_req_id
        self._request_waiters[req_id] = fut = asyncio.get_running_loop().create_future()
        await self.send(
            command=command,
            id=req_id,
            raise_errors=raise_errors,
            data=kwargs,
            **(top_level_data or {}),
        )
        return await fut

    def prepare(self, req: web.Request) -> Awaitable[None]:
        return self._ws.prepare(req)

    async def handle(self) -> None:
        try:
            self.log.debug(f"{self.type_name} opened (proto: {self.proto})")
            msg: WSMessage
            async for msg in self._ws:
                self.last_received = time.time()
                if msg.type == WSMsgType.ERROR:
                    self.log.error(f"Error in websocket connection", exc_info=self._ws.exception())
                    break
                elif msg.type == WSMsgType.CLOSE:
                    self.log.debug("Websocket close message received")
                    break
                elif msg.type == WSMsgType.TEXT:
                    if self.dead:
                        self.log.warning("Received data even though websocket is marked as dead")
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
