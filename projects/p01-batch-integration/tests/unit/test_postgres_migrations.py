"""Unit tests for deterministic PostgreSQL migration discovery."""

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from factoryflow_batch.adapters.exceptions import (
    PostgresMigrationDiscoveryError,
)
from factoryflow_batch.adapters.postgres_migrations import (
    discover_sql_migrations,
)


class TestDiscoverSqlMigrations(TestCase):
    """Validate migration artifact discovery without PostgreSQL."""

    def test_discovers_orders_and_hashes_migrations(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            second = root / "002_add_outbox_index.sql"
            first = root / "001_create_registry.sql"
            second.write_text("SELECT 2;\n", encoding="utf-8")
            first.write_text("SELECT 1;\n", encoding="utf-8")

            migrations = discover_sql_migrations(root)

            self.assertEqual(
                [migration.version for migration in migrations],
                [1, 2],
            )
            self.assertEqual(migrations[0].name, "create_registry")
            self.assertEqual(
                migrations[0].checksum,
                hashlib.sha256(first.read_bytes()).hexdigest(),
            )
            self.assertEqual(migrations[0].path, first.resolve())
            self.assertEqual(migrations[0].statement, "SELECT 1;\n")

    def test_ignores_non_sql_files(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text("documentation", encoding="utf-8")

            self.assertEqual(discover_sql_migrations(root), ())

    def test_rejects_invalid_directory(self) -> None:
        with TemporaryDirectory() as directory:
            missing = Path(directory) / "missing"

            with self.assertRaisesRegex(
                PostgresMigrationDiscoveryError,
                "does not exist",
            ):
                discover_sql_migrations(missing)

    def test_rejects_invalid_migration_artifacts(self) -> None:
        cases = {
            "000_reserved.sql": "reserved",
            "1_short_version.sql": "filename",
            "001-UPPERCASE.sql": "filename",
            "001_x.sql": "filename",
            "001_empty.sql": "empty",
        }

        for filename, expected_message in cases.items():
            with self.subTest(filename=filename), TemporaryDirectory() as directory:
                root = Path(directory)
                content = "" if filename == "001_empty.sql" else "SELECT 1;"
                (root / filename).write_text(content, encoding="utf-8")

                with self.assertRaisesRegex(
                    PostgresMigrationDiscoveryError,
                    expected_message,
                ):
                    discover_sql_migrations(root)

    def test_rejects_duplicate_versions(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "001_create_registry.sql").write_text(
                "SELECT 1;",
                encoding="utf-8",
            )
            (root / "001_create_outbox.sql").write_text(
                "SELECT 2;",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                PostgresMigrationDiscoveryError,
                "duplicated",
            ):
                discover_sql_migrations(root)

    def test_rejects_non_utf8_migration(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "001_invalid_encoding.sql").write_bytes(b"\xff\xfe")

            with self.assertRaisesRegex(
                PostgresMigrationDiscoveryError,
                "not readable UTF-8",
            ):
                discover_sql_migrations(root)

    def test_rejects_symbolic_link_migration(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.txt"
            target.write_text("SELECT 1;", encoding="utf-8")
            link = root / "001_linked_migration.sql"
            link.symlink_to(target.name)

            with self.assertRaisesRegex(
                PostgresMigrationDiscoveryError,
                "regular file",
            ):
                discover_sql_migrations(root)
