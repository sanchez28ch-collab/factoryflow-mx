"""Tests for the streaming local CSV inspection adapter."""

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from factoryflow_batch.adapters import (
    CsvArtifactValidationError,
    LocalArtifactAccessError,
    LocalCsvArtifactInspector,
)


class TestLocalCsvArtifactInspector(TestCase):
    """Verify streaming, structural validation and filesystem boundaries."""

    def setUp(self) -> None:
        self.temporary_directory = TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)

        self.base_root = Path(self.temporary_directory.name)
        self.allowed_root = self.base_root / "incoming"
        self.allowed_root.mkdir()

        self.inspector = LocalCsvArtifactInspector(
            self.allowed_root,
            chunk_size=7,
        )

    def _write(
        self,
        name: str,
        payload: bytes,
    ) -> Path:
        path = self.allowed_root / name
        path.write_bytes(payload)
        return path

    def test_inspects_utf8_bom_multiline_csv_in_one_stream(self) -> None:
        payload = (
            b'\xef\xbb\xbforder_id,description,quantity\r\n1,"reinforced\npanel",2\r\n2,base,1\r\n'
        )
        source = self._write(
            "production orders.csv",
            payload,
        )

        result = self.inspector.inspect(source.as_uri())

        self.assertEqual(
            result.source_file_name,
            "production orders.csv",
        )
        self.assertEqual(result.content_type, "text/csv")
        self.assertEqual(result.record_count, 2)
        self.assertEqual(result.file_size_bytes, len(payload))
        self.assertEqual(
            result.sha256,
            hashlib.sha256(payload).hexdigest(),
        )

    def test_ignores_physically_blank_rows(self) -> None:
        source = self._write(
            "orders.csv",
            b"id,name\n\n,\n1,Alice\n2,Bob\n",
        )

        result = self.inspector.inspect(source.as_uri())

        self.assertEqual(result.record_count, 2)

    def test_supports_configured_delimiter(self) -> None:
        source = self._write(
            "orders.csv",
            b"id;name\n1;Alice\n2;Bob\n",
        )
        inspector = LocalCsvArtifactInspector(
            self.allowed_root,
            delimiter=";",
            chunk_size=5,
        )

        result = inspector.inspect(source.as_uri())

        self.assertEqual(result.record_count, 2)

    def test_rejects_structurally_invalid_csv_files(self) -> None:
        cases = (
            (
                "empty.csv",
                b"",
                "must contain a header row",
            ),
            (
                "blank-header.csv",
                b"id,,quantity\n1,panel,2\n",
                "blank column names",
            ),
            (
                "duplicate-header.csv",
                b"Order_ID,order_id\n1,2\n",
                "duplicate column names",
            ),
            (
                "wrong-columns.csv",
                b"id,name\n1,Alice,unexpected\n",
                "row 2 has 3 columns; expected 2",
            ),
            (
                "invalid-syntax.csv",
                b'id,name\n1,"unterminated\n',
                "invalid CSV syntax",
            ),
        )

        for name, payload, expected_message in cases:
            with self.subTest(name=name):
                source = self._write(name, payload)

                with self.assertRaisesRegex(
                    CsvArtifactValidationError,
                    expected_message,
                ):
                    self.inspector.inspect(source.as_uri())

    def test_rejects_invalid_encoding(self) -> None:
        source = self._write(
            "invalid-encoding.csv",
            b"id,name\n1,\xff\n",
        )

        with self.assertRaisesRegex(
            CsvArtifactValidationError,
            "not valid for the configured encoding",
        ):
            self.inspector.inspect(source.as_uri())

    def test_rejects_invalid_source_uris(self) -> None:
        cases = (
            123,
            "https://example.com/orders.csv",
            "file://remote-server/tmp/orders.csv",
            "file:orders.csv",
            "file:///tmp/orders.csv?token=secret",
            "file:///tmp/orders.csv#fragment",
        )

        for source_uri in cases:
            with (
                self.subTest(source_uri=source_uri),
                self.assertRaises(LocalArtifactAccessError),
            ):
                self.inspector.inspect(source_uri)

    def test_rejects_missing_source(self) -> None:
        missing_source = self.allowed_root / "missing.csv"

        with self.assertRaisesRegex(
            LocalArtifactAccessError,
            "does not exist or cannot be resolved",
        ):
            self.inspector.inspect(missing_source.as_uri())

    def test_rejects_source_outside_allowed_root(self) -> None:
        outside_source = self.base_root / "outside.csv"
        outside_source.write_bytes(b"id,name\n1,Alice\n")

        with self.assertRaisesRegex(
            LocalArtifactAccessError,
            "outside the configured root",
        ):
            self.inspector.inspect(outside_source.as_uri())

    def test_rejects_symlink_resolving_outside_root(self) -> None:
        outside_source = self.base_root / "outside.csv"
        outside_source.write_bytes(b"id,name\n1,Alice\n")

        linked_source = self.allowed_root / "linked.csv"
        linked_source.symlink_to(outside_source)

        with self.assertRaisesRegex(
            LocalArtifactAccessError,
            "outside the configured root",
        ):
            self.inspector.inspect(linked_source.as_uri())

    def test_rejects_directory_as_source(self) -> None:
        with self.assertRaisesRegex(
            LocalArtifactAccessError,
            "could not be read",
        ):
            self.inspector.inspect(self.allowed_root.as_uri())

    def test_validates_constructor_configuration(self) -> None:
        missing_root = self.base_root / "missing-root"

        with self.assertRaisesRegex(
            ValueError,
            "allowed_root must exist",
        ):
            LocalCsvArtifactInspector(missing_root)

        regular_file = self.base_root / "regular-file"
        regular_file.write_text("not a directory", encoding="utf-8")

        with self.assertRaisesRegex(
            ValueError,
            "allowed_root must be a directory",
        ):
            LocalCsvArtifactInspector(regular_file)

        with self.assertRaisesRegex(
            ValueError,
            "registered codec",
        ):
            LocalCsvArtifactInspector(
                self.allowed_root,
                encoding="not-a-real-codec",
            )

        for delimiter in ("", "\n", "\r", "\0", "||"):
            with (
                self.subTest(delimiter=delimiter),
                self.assertRaisesRegex(
                    ValueError,
                    "one safe character",
                ),
            ):
                LocalCsvArtifactInspector(
                    self.allowed_root,
                    delimiter=delimiter,
                )

        for chunk_size in (True, "1024", 0, -1):
            with (
                self.subTest(chunk_size=chunk_size),
                self.assertRaisesRegex(
                    ValueError,
                    "positive integer",
                ),
            ):
                LocalCsvArtifactInspector(
                    self.allowed_root,
                    chunk_size=chunk_size,
                )
