"""Atomic immutable landing-store adapter for a local POSIX filesystem."""

import hashlib
import os
import re
import stat
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import unquote, urlsplit

from factoryflow_batch.adapters.exceptions import (
    ImmutableLandingConflictError,
    LocalArtifactAccessError,
    LocalArtifactIntegrityError,
)
from factoryflow_batch.ports import LandingReceipt

_DEFAULT_CHUNK_SIZE = 1024 * 1024
_DIRECTORY_MODE = 0o750
_FILE_MODE = 0o640
_SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")


class LocalFilesystemLandingStore:
    """Land files atomically without overwriting existing objects."""

    def __init__(
        self,
        *,
        source_root: Path,
        landing_root: Path,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
    ) -> None:
        try:
            resolved_source_root = source_root.resolve(strict=True)
        except OSError as error:
            raise ValueError("source_root must exist.") from error

        if not resolved_source_root.is_dir():
            raise ValueError("source_root must be a directory.")

        if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer.")

        try:
            landing_root.mkdir(
                mode=_DIRECTORY_MODE,
                parents=True,
                exist_ok=True,
            )
            resolved_landing_root = landing_root.resolve(strict=True)
        except OSError as error:
            raise ValueError("landing_root could not be created or resolved.") from error

        if not resolved_landing_root.is_dir():
            raise ValueError("landing_root must be a directory.")

        if resolved_source_root == resolved_landing_root:
            raise ValueError("source_root and landing_root must be different.")

        self._source_root = resolved_source_root
        self._landing_root = resolved_landing_root
        self._chunk_size = chunk_size

    def land(
        self,
        *,
        source_uri: str,
        object_key: str,
        expected_sha256: str,
    ) -> LandingReceipt:
        """Copy, verify and atomically publish one immutable object."""

        if not isinstance(expected_sha256, str) or not _SHA256_PATTERN.fullmatch(expected_sha256):
            raise LocalArtifactIntegrityError("expected_sha256 must be a lowercase SHA-256 digest.")

        source_path = self._resolve_source_path(source_uri)
        destination = self._resolve_destination(object_key)

        try:
            destination.parent.mkdir(
                mode=_DIRECTORY_MODE,
                parents=True,
                exist_ok=True,
            )
        except OSError as error:
            raise LocalArtifactAccessError("Landing destination could not be prepared.") from error

        if os.path.lexists(destination):
            return self._receipt_for_existing(
                destination=destination,
                expected_sha256=expected_sha256,
            )

        temporary_path: Path | None = None

        try:
            with (
                source_path.open("rb") as source,
                NamedTemporaryFile(
                    mode="wb",
                    prefix=f".{destination.name}.",
                    suffix=".tmp",
                    dir=destination.parent,
                    delete=False,
                ) as temporary,
            ):
                temporary_path = Path(temporary.name)
                os.chmod(temporary_path, _FILE_MODE)

                initial_source_stat = os.fstat(source.fileno())
                if not stat.S_ISREG(initial_source_stat.st_mode):
                    raise LocalArtifactAccessError("Landing source must be a regular file.")

                digest = hashlib.sha256()
                file_size_bytes = 0

                while True:
                    chunk = source.read(self._chunk_size)
                    if not chunk:
                        break

                    written = temporary.write(chunk)
                    if written != len(chunk):
                        raise LocalArtifactAccessError(
                            "Landing temporary file accepted a partial write."
                        )

                    digest.update(chunk)
                    file_size_bytes += len(chunk)

                temporary.flush()
                os.fsync(temporary.fileno())
                final_source_stat = os.fstat(source.fileno())

            source_identity_before = (
                initial_source_stat.st_dev,
                initial_source_stat.st_ino,
                initial_source_stat.st_size,
                initial_source_stat.st_mtime_ns,
            )
            source_identity_after = (
                final_source_stat.st_dev,
                final_source_stat.st_ino,
                final_source_stat.st_size,
                final_source_stat.st_mtime_ns,
            )

            if source_identity_before != source_identity_after:
                raise LocalArtifactAccessError("Landing source changed while it was being copied.")

            calculated_sha256 = digest.hexdigest()
            if calculated_sha256 != expected_sha256:
                raise LocalArtifactIntegrityError(
                    "Landing source SHA-256 differs from expected_sha256."
                )

            if file_size_bytes != final_source_stat.st_size:
                raise LocalArtifactIntegrityError("Landing source size changed during copy.")

            try:
                os.link(temporary_path, destination)
            except FileExistsError:
                return self._receipt_for_existing(
                    destination=destination,
                    expected_sha256=expected_sha256,
                )

            self._fsync_directory(destination.parent)

            destination_stat = destination.stat()
            return LandingReceipt(
                landing_uri=destination.as_uri(),
                landed_at=datetime.fromtimestamp(
                    destination_stat.st_mtime,
                    tz=UTC,
                ),
                file_size_bytes=file_size_bytes,
                sha256=calculated_sha256,
            )

        except (
            ImmutableLandingConflictError,
            LocalArtifactAccessError,
            LocalArtifactIntegrityError,
        ):
            raise
        except OSError as error:
            raise LocalArtifactAccessError("Artifact could not be landed safely.") from error
        finally:
            if temporary_path is not None:
                with suppress(OSError):
                    temporary_path.unlink(missing_ok=True)

    def _resolve_source_path(self, source_uri: str) -> Path:
        if not isinstance(source_uri, str):
            raise LocalArtifactAccessError("Landing source URI must be a string.")

        parsed_uri = urlsplit(source_uri)
        if (
            parsed_uri.scheme != "file"
            or parsed_uri.netloc
            or parsed_uri.query
            or parsed_uri.fragment
            or not parsed_uri.path.startswith("/")
        ):
            raise LocalArtifactAccessError("Landing source must be an absolute local file URI.")

        try:
            source_path = Path(unquote(parsed_uri.path)).resolve(strict=True)
        except (OSError, ValueError) as error:
            raise LocalArtifactAccessError(
                "Landing source does not exist or cannot be resolved."
            ) from error

        try:
            source_path.relative_to(self._source_root)
        except ValueError as error:
            raise LocalArtifactAccessError(
                "Landing source is outside the configured root."
            ) from error

        return source_path

    def _resolve_destination(self, object_key: str) -> Path:
        if not isinstance(object_key, str):
            raise LocalArtifactAccessError("object_key must be a string.")

        if (
            object_key != object_key.strip()
            or "\\" in object_key
            or any(ord(character) < 32 for character in object_key)
        ):
            raise LocalArtifactAccessError("object_key contains unsafe characters.")

        segments = object_key.split("/")
        if not segments or any(not segment or segment in {".", ".."} for segment in segments):
            raise LocalArtifactAccessError("object_key must contain safe relative segments.")

        destination = self._landing_root.joinpath(*segments)

        try:
            resolved_parent = destination.parent.resolve(strict=False)
            resolved_parent.relative_to(self._landing_root)
        except (OSError, ValueError) as error:
            raise LocalArtifactAccessError(
                "object_key resolves outside the landing root."
            ) from error

        return resolved_parent / destination.name

    def _receipt_for_existing(
        self,
        *,
        destination: Path,
        expected_sha256: str,
    ) -> LandingReceipt:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor: int | None = None

        try:
            destination_stat = destination.lstat()
            if not stat.S_ISREG(destination_stat.st_mode):
                raise ImmutableLandingConflictError("Immutable destination is not a regular file.")

            descriptor = os.open(destination, flags)
            opened_stat = os.fstat(descriptor)
            if not stat.S_ISREG(opened_stat.st_mode):
                raise ImmutableLandingConflictError("Immutable destination is not a regular file.")

            existing = os.fdopen(descriptor, "rb")
            descriptor = None

            with existing:
                initial_stat = opened_stat
                digest = hashlib.sha256()
                file_size_bytes = 0

                while True:
                    chunk = existing.read(self._chunk_size)
                    if not chunk:
                        break

                    digest.update(chunk)
                    file_size_bytes += len(chunk)

                final_stat = os.fstat(existing.fileno())

        except ImmutableLandingConflictError:
            raise
        except OSError as error:
            raise ImmutableLandingConflictError(
                "Immutable destination could not be verified."
            ) from error
        finally:
            if descriptor is not None:
                with suppress(OSError):
                    os.close(descriptor)

        identity_before = (
            initial_stat.st_dev,
            initial_stat.st_ino,
            initial_stat.st_size,
            initial_stat.st_mtime_ns,
        )
        identity_after = (
            final_stat.st_dev,
            final_stat.st_ino,
            final_stat.st_size,
            final_stat.st_mtime_ns,
        )

        if identity_before != identity_after:
            raise ImmutableLandingConflictError(
                "Immutable destination changed during verification."
            )

        calculated_sha256 = digest.hexdigest()
        if calculated_sha256 != expected_sha256 or file_size_bytes != final_stat.st_size:
            raise ImmutableLandingConflictError("Immutable destination contains different bytes.")

        return LandingReceipt(
            landing_uri=destination.as_uri(),
            landed_at=datetime.fromtimestamp(
                final_stat.st_mtime,
                tz=UTC,
            ),
            file_size_bytes=file_size_bytes,
            sha256=calculated_sha256,
        )

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        descriptor = os.open(
            directory,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
