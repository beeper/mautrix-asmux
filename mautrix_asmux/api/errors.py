# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Any
import json

from aiohttp import web


def _make_error(
    error_cls: type[web.HTTPError],
    errcode: str,
    error: str,
) -> web.HTTPError:
    return error_cls(
        text=json.dumps({"error": error, "errcode": errcode}),
        content_type="application/json",
    )


class _ErrorMeta(type):
    @property
    def request_not_json(self) -> web.HTTPException:
        return _make_error(
            web.HTTPBadRequest,
            "M_NOT_JSON",
            "Request body is not valid JSON",
        )

    @property
    def missing_appservice_id_query(self) -> web.HTTPException:
        return _make_error(
            web.HTTPBadRequest,
            "M_MISSING_PARAM",
            "Missing appservice_id query param",
        )

    @property
    def invalid_appservice_id_query(self) -> web.HTTPException:
        return _make_error(
            web.HTTPBadRequest, "M_INVALID_PARAM", "Malformed appservice_id query param"
        )

    @property
    def missing_auth_header(self) -> web.HTTPException:
        return _make_error(web.HTTPForbidden, "M_MISSING_TOKEN", "Missing authorization header")

    @property
    def invalid_auth_header(self) -> web.HTTPException:
        return _make_error(web.HTTPForbidden, "M_UNKNOWN_TOKEN", "Invalid authorization header")

    @property
    def invalid_auth_token(self) -> web.HTTPException:
        return _make_error(web.HTTPForbidden, "M_UNKNOWN_TOKEN", "Invalid authorization token")

    @property
    def invalid_config_password(self) -> web.HTTPException:
        return _make_error(
            web.HTTPForbidden, "M_UNKNOWN_TOKEN", "Invalid config download password"
        )

    @property
    def config_download_unsupported(self) -> web.HTTPException:
        return _make_error(
            web.HTTPNotFound,
            "COM.BEEPER.NO_CONFIG_DOWNLOAD",
            "That bridge does not downloading generated configs",
        )

    @property
    def appservice_access_denied(self) -> web.HTTPException:
        return _make_error(
            web.HTTPUnauthorized,
            "M_UNAUTHORIZED",
            "You are not authorized to access that appservice",
        )

    @property
    def room_delete_access_denied(self) -> web.HTTPException:
        return _make_error(
            web.HTTPUnauthorized, "M_UNAUTHORIZED", "You are not authorized to delete rooms"
        )

    @property
    def room_not_found(self) -> web.HTTPException:
        return _make_error(web.HTTPNotFound, "M_NOT_FOUND", "Unknown room ID")

    @property
    def appservice_ws_not_enabled(self) -> web.HTTPException:
        return _make_error(
            web.HTTPUnauthorized,
            "COM.BEEPER.TXN_WS_NOT_ENABLED",
            "This appservice is not marked to use websocket for receiving transactions.",
        )

    @property
    def server_shutting_down(self) -> web.HTTPException:
        return _make_error(
            web.HTTPServiceUnavailable,
            "COM.BEEPER.SHUTTING_DOWN",
            "The server is shutting down. Please try again later.",
        )

    @property
    def user_access_denied(self) -> web.HTTPException:
        return _make_error(
            web.HTTPUnauthorized, "M_UNAUTHORIZED", "You are not authorized to access that user"
        )

    @property
    def invalid_user_id(self) -> web.HTTPException:
        return _make_error(
            web.HTTPForbidden,
            "M_FORBIDDEN",
            "Application service cannot masquerade as this local user.",
        )

    @property
    def external_user_id(self) -> web.HTTPException:
        return _make_error(
            web.HTTPForbidden,
            "M_FORBIDDEN",
            "Application service cannot masquerade as user on external homeserver.",
        )

    @property
    def invalid_owner(self) -> web.HTTPException:
        return _make_error(
            web.HTTPBadRequest, "M_BAD_REQUEST", "Appservice owner must match [a-z0-9=.-]{1,32}"
        )

    @property
    def invalid_prefix(self) -> web.HTTPException:
        return _make_error(
            web.HTTPBadRequest, "M_BAD_REQUEST", "Appservice prefix must match [a-z0-9=.-]{1,32}"
        )

    @property
    def invalid_uuid(self) -> web.HTTPException:
        return _make_error(web.HTTPBadRequest, "M_BAD_REQUEST", "Invalid UUID")

    @property
    def missing_fields(self) -> web.HTTPException:
        return _make_error(
            web.HTTPBadRequest, "M_BAD_REQUEST", "Missing one or more fields in request"
        )

    @property
    def bridge_query_required(self) -> web.HTTPException:
        return _make_error(web.HTTPBadRequest, "M_BAD_REQUEST", "`bridge` query param is required")

    @property
    def mismatching_user(self) -> web.HTTPException:
        return _make_error(
            web.HTTPBadRequest,
            "M_FORBIDDEN",
            "Application service shouldn't have that user's access token",
        )

    @property
    def appservice_not_found(self) -> web.HTTPException:
        return _make_error(web.HTTPNotFound, "M_NOT_FOUND", "Application service not found.")

    @property
    def user_not_found(self) -> web.HTTPException:
        return _make_error(web.HTTPNotFound, "M_NOT_FOUND", "User not found.")

    @property
    def proxy_not_setup(self) -> web.HTTPException:
        return _make_error(web.HTTPNotFound, "M_NOT_FOUND", "Proxy config not set.")

    @property
    def failed_to_contact_homeserver(self) -> web.HTTPException:
        return _make_error(web.HTTPBadGateway, "M_BAD_GATEWAY", "Failed to contact homeserver")

    @property
    def failed_to_register_bot(self) -> web.HTTPException:
        return _make_error(
            web.HTTPInternalServerError, "M_UNKNOWN", "Failed to register bridge bot"
        )

    @property
    def websocket_not_connected(self) -> web.HTTPException:
        raise _make_error(
            web.HTTPBadGateway, "FI.MAU.WS_NOT_CONNECTED", "Endpoint is not connected to websocket"
        )

    @property
    def syncproxy_error_not_supported(self) -> web.HTTPException:
        raise _make_error(
            web.HTTPNotImplemented,
            "FI.MAU.NOT_IMPLEMENTED",
            "Sending syncproxy errors to non-websocket appservices is not implemented",
        )

    @property
    def exec_not_supported(self) -> web.HTTPException:
        raise _make_error(
            web.HTTPNotImplemented,
            "FI.MAU.NOT_IMPLEMENTED",
            "Sending custom API requests to non-websocket appservices is not implemented",
        )


class Error(metaclass=_ErrorMeta):
    pass


class WebsocketClosedError(Exception):
    def __init__(self) -> None:
        super().__init__("Websocket closed before response received")


class WebsocketErrorResponse(Exception):
    def __init__(self, data: dict[str, Any]) -> None:
        message = data["message"]
        if "code" in data:
            message = f"{data['code']}: {message}"
        super().__init__(message)
        self.data = data


class WebsocketNotConnected(Exception):
    def __init__(self) -> None:
        super().__init__("Websocket not connected")
