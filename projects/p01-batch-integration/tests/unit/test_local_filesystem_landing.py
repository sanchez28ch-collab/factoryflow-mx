"""Tests for atomic immutable local filesystem landing."""

import hashlib
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from urllib.parse import unquote, urlsplit

from factoryflow_batch.adapters import (
    ImmutableLandingConflictError,
    LocalArtifactAccessError,
    LocalArtifactIntegrityError,
    LocalFilesystemLandingStore,
)


class TestLocalFilesystemLandingStore(TestCase):
    """Verify durability, immutability and containment guarantees."""

    def setUp(self) -> None:
        self.temporary_directory = TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)

        self.base_root = Path(self.temporary_directory.name)
        self.source_root = self.base_root / "incoming"
        self.landing_root = self.base_root / "landing"
        self.source_root.mkdir()

        self.payload = b"order_id,product,quantity\n1001,panel,2\n1002,base,1\n"
        self.source = self.source_root / "orders.csv"
        self.source.write_bytes(self.payload)
        self.sha256 = hashlib.sha256(self.payload).hexdigest()
        self.object_key = "raw/erp/2026-09-05/orders.csv"

        self.store = LocalFilesystemLandingStore(
            source_root=self.source_root,
            landing_root=self.landing_root,
            chunk_size=7,
        )

    @staticmethod
    def _receipt_path(landing_uri: str) -> Path:
        parsed_uri = urlsplit(landing_uri)
        return Path(unquote(parsed_uri.path))

    def test_lands_file_atomically_with_restricted_permissions(self) -> None:
        receipt = self.store.land(
            source_uri=self.source.as_uri(),
            object_key=self.object_key,
            expected_sha256=self.sha256,
        )
        destination = self._receipt_path(receipt.landing_uri)

        self.assertEqual(destination.read_bytes(), self.payload)
        self.assertEqual(receipt.file_size_bytes, len(self.payload))
        self.assertEqual(receipt.sha256, self.sha256)
        self.assertIsNotNone(receipt.landed_at.tzinfo)
        self.assertEqual(
            stat.S_IMODE(destination.stat().st_mode),
            0o640,
        )
        self.assertEqual(
            list(destination.parent.glob(f".{destination.name}.*.tmp")),
            [],
        )

    def test_retry_returns_same_immutable_receipt(self) -> None:
        first = self.store.land(
            source_uri=self.source.as_uri(),
            object_key=self.object_key,
            expected_sha256=self.sha256,
        )
        second = self.store.land(
            source_uri=self.source.as_uri(),
            object_key=self.object_key,
            expected_sha256=self.sha256,
        )

        self.assertEqual(second, first)
        self.assertEqual(
            self._receipt_path(second.landing_uri).read_bytes(),
            self.payload,
        )

    def test_concurrent_retries_publish_one_identical_object(self) -> None:
        def land_once(_: int) -> str:
            receipt = self.store.land(
                source_uri=self.source.as_uri(),
                object_key=self.object_key,
                expected_sha256=self.sha256,
            )
            return receipt.landing_uri

        with ThreadPoolExecutor(max_workers=8) as executor:
            landing_uris = list(executor.map(land_once, range(16)))

        self.assertEqual(len(set(landing_uris)), 1)
        destination = self._receipt_path(landing_uris[0])
        self.assertEqual(destination.read_bytes(), self.payload)

        landed_files = [path for path in self.landing_root.rglob("*") if path.is_file()]
        self.assertEqual(landed_files, [destination])

    def test_checksum_mismatch_does_not_publish_destination(self) -> None:
        with self.assertRaisesRegex(
            LocalArtifactIntegrityError,
            "differs from expected_sha256",
        ):
            self.store.land(
                source_uri=self.source.as_uri(),
                object_key=self.object_key,
                expected_sha256="b" * 64,
            )

        destination = self.landing_root / self.object_key
        self.assertFalse(destination.exists())
        self.assertEqual(
            list(destination.parent.glob("*.tmp")),
            [],
        )
        self.assertEqual(
            list(destination.parent.glob(".*.tmp")),
            [],
        )

    def test_rejects_existing_destination_with_different_bytes(
        self,
    ) -> None:
        destination = self.landing_root / self.object_key
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"different bytes")

        with self.assertRaisesRegex(
            ImmutableLandingConflictError,
            "contains different bytes",
        ):
            self.store.land(
                source_uri=self.source.as_uri(),
                object_key=self.object_key,
                expected_sha256=self.sha256,
            )

        self.assertEqual(destination.read_bytes(), b"different bytes")

    def test_rejects_symlink_destination(self) -> None:
        destination = self.landing_root / self.object_key
        destination.parent.mkdir(parents=True)

        outside = self.base_root / "outside-destination.csv"
        outside.write_bytes(self.payload)
        destination.symlink_to(outside)

        with self.assertRaisesRegex(
            ImmutableLandingConflictError,
            "not a regular file",
        ):
            self.store.land(
                source_uri=self.source.as_uri(),
                object_key=self.object_key,
                expected_sha256=self.sha256,
            )

    def test_rejects_directory_destination(self) -> None:
        destination = self.landing_root / self.object_key
        destination.mkdir(parents=True)

        with self.assertRaisesRegex(
            ImmutableLandingConflictError,
            "not a regular file",
        ):
            self.store.land(
                source_uri=self.source.as_uri(),
                object_key=self.object_key,
                expected_sha256=self.sha256,
            )

    def test_rejects_invalid_expected_digests(self) -> None:
        invalid_digests = (
            123,
            "",
            "A" * 64,
            "a" * 63,
            "not-a-digest",
        )

        for expected_sha256 in invalid_digests:
            with (
                self.subTest(expected_sha256=expected_sha256),
                self.assertRaisesRegex(
                    LocalArtifactIntegrityError,
                    "lowercase SHA-256",
                ),
            ):
                self.store.land(
                    source_uri=self.source.as_uri(),
                    object_key=self.object_key,
                    expected_sha256=expected_sha256,
                )

    def test_rejects_unsafe_object_keys(self) -> None:
        invalid_object_keys = (
            123,
            "",
            "/absolute.csv",
            "raw//orders.csv",
            "raw/../orders.csv",
            "raw/./orders.csv",
            r"raw\orders.csv",
            " raw/orders.csv",
            "raw/orders.csv\n",
        )

        for object_key in invalid_object_keys:
            with (
                self.subTest(object_key=object_key),
                self.assertRaises(LocalArtifactAccessError),
            ):
                self.store.land(
                    source_uri=self.source.as_uri(),
                    object_key=object_key,
                    expected_sha256=self.sha256,
                )

    def test_rejects_destination_symlink_escape(self) -> None:
        outside_root = self.base_root / "outside-landing"
        outside_root.mkdir()

        escaped_segment = self.landing_root / "escaped"
        escaped_segment.symlink_to(outside_root)

        with self.assertRaisesRegex(
            LocalArtifactAccessError,
            "outside the landing root",
        ):
            self.store.land(
                source_uri=self.source.as_uri(),
                object_key="escaped/orders.csv",
                expected_sha256=self.sha256,
            )

    def test_rejects_invalid_sources(self) -> None:
        outside_source = self.base_root / "outside-source.csv"
        outside_source.write_bytes(self.payload)

        missing_source = self.source_root / "missing.csv"

        invalid_sources = (
            123,
            "https://example.com/orders.csv",
            "file://remote-server/tmp/orders.csv",
            "file:orders.csv",
            f"{self.source.as_uri()}?token=secret",
            missing_source.as_uri(),
            outside_source.as_uri(),
            self.source_root.as_uri(),
        )

        for source_uri in invalid_sources:
            with (
                self.subTest(source_uri=source_uri),
                self.assertRaises(LocalArtifactAccessError),
            ):
                self.store.land(
                    source_uri=source_uri,
                    object_key=self.object_key,
                    expected_sha256=self.sha256,
                )

    def test_validates_constructor_configuration(self) -> None:
        missing_root = self.base_root / "missing-source-root"

        with self.assertRaisesRegex(
            ValueError,
            "source_root must exist",
        ):
            LocalFilesystemLandingStore(
                source_root=missing_root,
                landing_root=self.landing_root,
            )

        regular_file = self.base_root / "regular-file"
        regular_file.write_text("data", encoding="utf-8")

        with self.assertRaisesRegex(
            ValueError,
            "source_root must be a directory",
        ):
            LocalFilesystemLandingStore(
                source_root=regular_file,
                landing_root=self.landing_root,
            )

        landing_file = self.base_root / "landing-file"
        landing_file.write_text("data", encoding="utf-8")

        with self.assertRaisesRegex(
            ValueError,
            "could not be created or resolved",
        ):
            LocalFilesystemLandingStore(
                source_root=self.source_root,
                landing_root=landing_file,
            )

        with self.assertRaisesRegex(
            ValueError,
            "must be different",
        ):
            LocalFilesystemLandingStore(
                source_root=self.source_root,
                landing_root=self.source_root,
            )

        for chunk_size in (True, "1024", 0, -1):
            with (
                self.subTest(chunk_size=chunk_size),
                self.assertRaisesRegex(
                    ValueError,
                    "positive integer",
                ),
            ):
                LocalFilesystemLandingStore(
                    source_root=self.source_root,
                    landing_root=self.landing_root,
                    chunk_size=chunk_size,
                )

    def test_destination_is_created_inside_landing_root(self) -> None:
        receipt = self.store.land(
            source_uri=self.source.as_uri(),
            object_key=self.object_key,
            expected_sha256=self.sha256,
        )
        destination = self._receipt_path(receipt.landing_uri)

        self.assertTrue(destination.is_relative_to(self.landing_root.resolve()))
        self.assertFalse(os.path.lexists(self.base_root / "orders.csv"))
