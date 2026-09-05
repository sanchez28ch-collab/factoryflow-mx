"""Streaming CSV inspection adapter for local file URIs."""

import codecs
import csv
import hashlib
import os
import stat
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import unquote, urlsplit

from factoryflow_batch.adapters.exceptions import (
    CsvArtifactValidationError,
    LocalArtifactAccessError,
)
from factoryflow_batch.ports import InspectedArtifact

_DEFAULT_CHUNK_SIZE = 1024 * 1024


class LocalCsvArtifactInspector:
    """Inspect UTF-8 CSV files without loading complete artifacts into RAM."""

    def __init__(
        self,
        allowed_root: Path,
        *,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
    ) -> None:
        try:
            resolved_root = allowed_root.resolve(strict=True)
        except OSError as error:
            raise ValueError("allowed_root must exist.") from error

        if not resolved_root.is_dir():
            raise ValueError("allowed_root must be a directory.")

        try:
            codecs.lookup(encoding)
        except LookupError as error:
            raise ValueError("encoding must be a registered codec.") from error

        if len(delimiter) != 1 or delimiter in {"\r", "\n", "\0"}:
            raise ValueError("delimiter must be one safe character.")

        if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer.")

        self._allowed_root = resolved_root
        self._encoding = encoding
        self._delimiter = delimiter
        self._chunk_size = chunk_size

    def inspect(self, source_uri: str, /) -> InspectedArtifact:
        """Stream a CSV and return validated metadata and integrity values."""

        source_path = self._resolve_source_path(source_uri)
        digest = hashlib.sha256()
        file_size_bytes = 0

        try:
            with source_path.open("rb") as source:
                initial_stat = os.fstat(source.fileno())
                if not stat.S_ISREG(initial_stat.st_mode):
                    raise LocalArtifactAccessError("CSV source must be a regular file.")

                decoder_factory = codecs.getincrementaldecoder(self._encoding)
                decoder = decoder_factory(errors="strict")
                pending_text = ""

                def decoded_lines() -> Iterator[str]:
                    nonlocal file_size_bytes
                    nonlocal pending_text

                    while True:
                        chunk = source.read(self._chunk_size)
                        if not chunk:
                            break

                        digest.update(chunk)
                        file_size_bytes += len(chunk)
                        pending_text += decoder.decode(chunk)

                        physical_lines = pending_text.split("\n")
                        pending_text = physical_lines.pop()

                        for physical_line in physical_lines:
                            yield f"{physical_line}\n"

                    pending_text += decoder.decode(b"", final=True)
                    if pending_text:
                        yield pending_text

                reader = csv.reader(
                    decoded_lines(),
                    delimiter=self._delimiter,
                    strict=True,
                )
                header = next(reader, None)

                if header is None:
                    raise CsvArtifactValidationError("CSV artifact must contain a header row.")

                normalized_header = [column.strip() for column in header]
                if not normalized_header or any(not column for column in normalized_header):
                    raise CsvArtifactValidationError(
                        "CSV header cannot contain blank column names."
                    )

                normalized_keys = [column.casefold() for column in normalized_header]
                if len(normalized_keys) != len(set(normalized_keys)):
                    raise CsvArtifactValidationError("CSV header contains duplicate column names.")

                record_count = 0
                expected_columns = len(header)

                for row_number, row in enumerate(reader, start=2):
                    if not row or not any(value.strip() for value in row):
                        continue

                    if len(row) != expected_columns:
                        raise CsvArtifactValidationError(
                            "CSV row "
                            f"{row_number} has {len(row)} columns; "
                            f"expected {expected_columns}."
                        )

                    record_count += 1

                final_stat = os.fstat(source.fileno())

        except UnicodeDecodeError as error:
            raise CsvArtifactValidationError(
                "CSV artifact is not valid for the configured encoding."
            ) from error
        except csv.Error as error:
            raise CsvArtifactValidationError("CSV artifact contains invalid CSV syntax.") from error
        except OSError as error:
            raise LocalArtifactAccessError("CSV artifact could not be read.") from error

        initial_identity = (
            initial_stat.st_dev,
            initial_stat.st_ino,
            initial_stat.st_size,
            initial_stat.st_mtime_ns,
        )
        final_identity = (
            final_stat.st_dev,
            final_stat.st_ino,
            final_stat.st_size,
            final_stat.st_mtime_ns,
        )
        if initial_identity != final_identity:
            raise LocalArtifactAccessError("CSV artifact changed while it was being inspected.")

        if file_size_bytes != final_stat.st_size:
            raise LocalArtifactAccessError("CSV artifact size changed during inspection.")

        return InspectedArtifact(
            source_file_name=source_path.name,
            content_type="text/csv",
            record_count=record_count,
            file_size_bytes=file_size_bytes,
            sha256=digest.hexdigest(),
        )

    def _resolve_source_path(self, source_uri: str) -> Path:
        if not isinstance(source_uri, str):
            raise LocalArtifactAccessError("CSV source URI must be a string.")

        parsed_uri = urlsplit(source_uri)
        if (
            parsed_uri.scheme != "file"
            or parsed_uri.netloc
            or parsed_uri.query
            or parsed_uri.fragment
            or not parsed_uri.path.startswith("/")
        ):
            raise LocalArtifactAccessError("CSV source must be an absolute local file URI.")

        try:
            source_path = Path(unquote(parsed_uri.path)).resolve(strict=True)
        except (OSError, ValueError) as error:
            raise LocalArtifactAccessError(
                "CSV source does not exist or cannot be resolved."
            ) from error

        try:
            source_path.relative_to(self._allowed_root)
        except ValueError as error:
            raise LocalArtifactAccessError("CSV source is outside the configured root.") from error

        return source_path
