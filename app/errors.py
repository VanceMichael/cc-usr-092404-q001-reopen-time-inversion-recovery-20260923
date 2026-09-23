"""各层共享的稳定结构化错误类型。

所有 HTTP 错误均使用相同的响应结构：

    {"error": {"code": "...", "message": "...", "details": {...}}}

错误代码属于 API 契约，修改时必须保持兼容。
"""

from __future__ import annotations

from typing import Any


class AppError(Exception):
    """可转换为结构化 HTTP 响应的错误基类。"""

    code = "internal_error"
    status = 500

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details: dict[str, Any] = details or {}

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            body["details"] = self.details
        return {"error": body}


class ValidationError(AppError):
    code = "validation_error"
    status = 422


class UnknownAirportError(ValidationError):
    code = "unknown_airport"


class EventConflictError(AppError):
    code = "event_conflict"
    status = 409


class NotFoundError(AppError):
    code = "not_found"
    status = 404


class BadRequestError(AppError):
    code = "bad_request"
    status = 400


class UnsupportedMediaTypeError(BadRequestError):
    code = "unsupported_media_type"


class MethodNotAllowedError(AppError):
    code = "method_not_allowed"
    status = 405
