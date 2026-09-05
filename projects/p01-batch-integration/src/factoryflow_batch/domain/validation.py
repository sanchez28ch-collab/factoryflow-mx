"""Reusable validation primitives for the batch domain."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import NoReturn
from urllib.parse import urlsplit

from factoryflow_batch.domain.exceptions import BatchManifestValidationError

_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")

ALLOWED_CONTENT_TYPES = frozenset(
    {
        "text/csv",
        "application/json",
        "application/x-ndjson",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
)


def _invalid(field_name: str, reason: str) -> NoReturn:
    raise BatchManifestValidationError(f"{field_name} {reason}")


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        _invalid(field_name, "must be a string.")

    return value


def validated_name(value: object, field_name: str) -> str:
    candidate = _text(value, field_name)

    if not 3 <= len(candidate) <= 63:
        _invalid(field_name, "length must be between 3 and 63 characters.")

    if _NAME_PATTERN.fullmatch(candidate) is None:
        _invalid(field_name, "has an invalid format.")

    return candidate


def validated_version(value: object, field_name: str) -> str:
    candidate = _text(value, field_name)

    if _VERSION_PATTERN.fullmatch(candidate) is None:
        _invalid(field_name, "must use semantic version format X.Y.Z.")

    return candidate


def validated_sha256(value: object, field_name: str) -> str:
    candidate = _text(value, field_name)

    if _SHA256_PATTERN.fullmatch(candidate) is None:
        _invalid(field_name, "must contain 64 lowercase hexadecimal characters.")

    return candidate


def validated_logical_date(value: object) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        _invalid("logical_date", "must be a date without a time component.")

    return value


def validated_utc_datetime(
    value: object,
    field_name: str,
) -> datetime:
    if not isinstance(value, datetime):
        _invalid(field_name, "must be a datetime.")

    if value.tzinfo is None or value.utcoffset() is None:
        _invalid(field_name, "must include timezone information.")

    return value.astimezone(UTC)


def validated_file_name(value: object) -> str:
    candidate = _text(value, "source_file_name")

    if not 1 <= len(candidate) <= 255:
        _invalid("source_file_name", "length must be between 1 and 255.")

    if candidate in {".", ".."} or "/" in candidate or "\\" in candidate or "\x00" in candidate:
        _invalid("source_file_name", "must not contain a directory path.")

    return candidate


def validated_landing_uri(value: object) -> str:
    candidate = _text(value, "landing_uri")
    parsed = urlsplit(candidate)

    if parsed.query or parsed.fragment:
        _invalid("landing_uri", "must not contain a query or fragment.")

    if parsed.scheme == "gs":
        if not parsed.netloc or parsed.path in {"", "/"}:
            _invalid("landing_uri", "must contain a bucket and object path.")
        return candidate

    if parsed.scheme == "file":
        if parsed.netloc or not parsed.path.startswith("/"):
            _invalid("landing_uri", "must be an absolute local file URI.")
        return candidate

    _invalid("landing_uri", "must use the gs:// or file:/// scheme.")


def validated_content_type(value: object) -> str:
    candidate = _text(value, "content_type")

    if candidate not in ALLOWED_CONTENT_TYPES:
        _invalid("content_type", "is not supported.")

    return candidate


def validated_non_negative_integer(
    value: object,
    field_name: str,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _invalid(field_name, "must be an integer.")

    if value < 0:
        _invalid(field_name, "must not be negative.")

    return value


def validated_positive_integer(
    value: object,
    field_name: str,
) -> int:
    validated = validated_non_negative_integer(value, field_name)

    if validated == 0:
        _invalid(field_name, "must be greater than zero.")

    return validated
