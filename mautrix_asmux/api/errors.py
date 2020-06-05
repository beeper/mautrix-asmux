# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from typing import Dict
import json

from aiohttp import web


class _ErrorMeta:
    def __init__(self, *args, **kwargs) -> None:
        pass

    @staticmethod
    def _make_error(errcode: str, error: str) -> Dict[str, str]:
        return {
            "body": json.dumps({
                "error": error,
                "errcode": errcode,
            }),
            "content_type": "application/json",
        }

    @property
    def request_not_json(self) -> web.HTTPException:
        return web.HTTPBadRequest(**self._make_error("M_NOT_JSON",
                                                     "Request body is not valid JSON"))

    @property
    def missing_auth_header(self) -> web.HTTPException:
        return web.HTTPForbidden(**self._make_error("M_MISSING_TOKEN",
                                                    "Missing authorization header"))

    @property
    def invalid_auth_header(self) -> web.HTTPException:
        return web.HTTPForbidden(**self._make_error("M_UNKNOWN_TOKEN",
                                                    "Invalid authorization header"))

    @property
    def invalid_auth_token(self) -> web.HTTPException:
        return web.HTTPForbidden(**self._make_error("M_UNKNOWN_TOKEN",
                                                    "Invalid authorization token"))

    @property
    def appservice_access_denied(self) -> web.HTTPException:
        return web.HTTPUnauthorized(**self._make_error(
            "M_UNAUTHORIZED", "You are not authorized to access that appservice"))

    @property
    def user_access_denied(self) -> web.HTTPException:
        return web.HTTPUnauthorized(**self._make_error(
            "M_UNAUTHORIZED", "You are not authorized to access that user"))

    @property
    def invalid_user_id(self) -> web.HTTPException:
        return web.HTTPForbidden(**self._make_error(
            "M_FORBIDDEN", "Application service cannot masquerade as this local user."))

    @property
    def external_user_id(self) -> web.HTTPException:
        return web.HTTPForbidden(**self._make_error(
            "M_FORBIDDEN",
            "Application service cannot masquerade as user on external homeserver."))

    @property
    def invalid_owner(self) -> web.HTTPException:
        return web.HTTPBadRequest(**self._make_error(
            "M_BAD_REQUEST", "Appservice owner must match [a-z0-9=.-]{1,32}"))

    @property
    def invalid_prefix(self) -> web.HTTPException:
        return web.HTTPBadRequest(**self._make_error(
            "M_BAD_REQUEST", "Appservice prefix must match [a-z0-9=.-]{1,32}"))

    @property
    def missing_fields(self) -> web.HTTPException:
        return web.HTTPBadRequest(**self._make_error(
            "M_BAD_REQUEST", "Missing one or more fields in request"))

    @property
    def appservice_not_found(self) -> web.HTTPException:
        return web.HTTPNotFound(**self._make_error("M_NOT_FOUND",
                                                   "Application service not found."))

    @property
    def user_not_found(self) -> web.HTTPException:
        return web.HTTPNotFound(**self._make_error("M_NOT_FOUND",
                                                   "User not found."))

    @property
    def failed_to_contact_homeserver(self) -> web.HTTPException:
        return web.HTTPBadGateway(**self._make_error("M_BAD_GATEWAY",
                                                     "Failed to contact homeserver"))

    @property
    def failed_to_register_bot(self) -> web.HTTPException:
        return web.HTTPInternalServerError(**self._make_error("M_UNKNOWN",
                                                              "Failed to register bridge bot"))


class Error(metaclass=_ErrorMeta):
    pass
