"""Deterministic object-key policy for the immutable raw landing zone."""

from datetime import date
from pathlib import PurePosixPath


def build_landing_object_key(
    *,
    source_system: str,
    dataset: str,
    logical_date: date,
    schema_version: str,
    idempotency_key: str,
    source_file_name: str,
) -> str:
    """Build a partition-friendly and retry-safe raw object key."""

    if (
        not source_file_name
        or source_file_name in {".", ".."}
        or "/" in source_file_name
        or "\\" in source_file_name
    ):
        raise ValueError("source_file_name must be a safe base name.")

    suffix = PurePosixPath(source_file_name).suffix.lower()

    return "/".join(
        (
            "raw",
            f"source_system={source_system}",
            f"dataset={dataset}",
            f"logical_date={logical_date.isoformat()}",
            f"schema_version={schema_version}",
            f"{idempotency_key}{suffix}",
        )
    )
