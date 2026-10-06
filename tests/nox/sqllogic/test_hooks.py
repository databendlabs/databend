import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import hooks


class FakeConnection:
    def __init__(self, counts=None):
        self.counts = iter(counts or [])
        self.statements = []
        self.scripts = []

    def execute(self, sql):
        self.statements.append(sql)

    def execute_script(self, sql):
        self.scripts.append(sql)

    def count(self, sql):
        self.statements.append(sql)
        return next(self.counts)


class SqlLogicHooksTest(unittest.TestCase):
    def create_cache(self, root, *, kind="tpch", scale_factor=1):
        output = Path(root) / f"{kind}_{scale_factor}"
        output.mkdir()
        tables = hooks.TPCH_TABLES if kind == "tpch" else hooks.TPCDS_TABLES
        for table in tables:
            (output / f"{table}.csv").write_text(f"{table}\n")
        return output

    def test_duckdb_cache_requires_nonempty_table_files(self):
        with tempfile.TemporaryDirectory() as directory:
            output = self.create_cache(directory)
            self.assertTrue(hooks.duckdb_cache_is_valid(output, "tpch"))

            nation = output / "nation.csv"
            nation.unlink()
            self.assertFalse(hooks.duckdb_cache_is_valid(output, "tpch"))

            nation.write_text("")
            self.assertFalse(hooks.duckdb_cache_is_valid(output, "tpch"))

    def test_generate_duckdb_data_reuses_valid_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            output = self.create_cache(directory)
            duckdb = mock.Mock()
            with (
                mock.patch.object(hooks, "CACHE_DIR", Path(directory)),
                mock.patch.dict("sys.modules", {"duckdb": duckdb}),
            ):
                self.assertEqual(hooks.generate_duckdb_data("tpch", 1), output)

            duckdb.connect.assert_not_called()

    def test_generate_duckdb_data_rebuilds_invalid_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            output = self.create_cache(directory)
            (output / "nation.csv").unlink()
            connection = mock.Mock()

            def export_database(sql):
                if not sql.startswith("EXPORT DATABASE"):
                    return
                export = Path(sql.split("'", 2)[1])
                export.mkdir()
                for table in hooks.TPCH_TABLES:
                    (export / f"{table}.csv").write_text(f"{table}\n")

            connection.execute.side_effect = export_database
            duckdb = mock.Mock()
            duckdb.connect.return_value = connection
            with (
                mock.patch.object(hooks, "CACHE_DIR", Path(directory)),
                mock.patch.dict("sys.modules", {"duckdb": duckdb}),
            ):
                rebuilt = hooks.generate_duckdb_data("tpch", 1)

            self.assertEqual(rebuilt, output)
            self.assertTrue(hooks.duckdb_cache_is_valid(output, "tpch"))
            connection.close.assert_called_once_with()

    def test_databend_dsn_uses_local_handler_environment(self):
        with mock.patch.dict(
            os.environ,
            {
                "DATABEND_DSN": "databend://user:pass@remote:8000/db",
                "QUERY_MYSQL_HANDLER_HOST": "db.example",
                "QUERY_HTTP_HANDLER_PORT": "9000",
            },
            clear=True,
        ):
            self.assertEqual(
                hooks.databend_dsn(),
                "databend://root:@db.example:9000/?sslmode=disable",
            )

    def test_run_hook_closes_databend_connection(self):
        connection = mock.Mock()
        with (
            mock.patch.object(hooks, "DatabendConnection", return_value=connection),
            mock.patch.object(hooks, "prepare_stage", side_effect=RuntimeError("boom")),
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                hooks.run_hook("prepare", "stage")

        connection.close.assert_called_once_with()

    def test_data_is_loaded_skips_queries_when_force_load_is_enabled(self):
        connection = FakeConnection()
        with mock.patch.dict(
            os.environ, {"DATABEND_SQLLOGICTEST_FORCE_LOAD": "1"}, clear=False
        ):
            self.assertFalse(hooks.data_is_loaded(connection, "tpch_test", "nation"))
        self.assertEqual(connection.statements, [])

    def test_data_is_loaded_requires_an_existing_nonempty_table(self):
        missing = FakeConnection([0])
        self.assertFalse(hooks.data_is_loaded(missing, "tpch_test", "nation"))

        empty = FakeConnection([1, 0])
        self.assertFalse(hooks.data_is_loaded(empty, "tpch_test", "nation"))

        loaded = FakeConnection([1, 25])
        self.assertTrue(hooks.data_is_loaded(loaded, "tpch_test", "nation"))

    def test_prepare_tpch_uses_python_generated_data(self):
        connection = FakeConnection([0])
        with mock.patch.object(
            hooks, "generate_duckdb_data", return_value=Path("/tmp/tpch_1")
        ) as generate:
            hooks.prepare_tpch(connection)

        generate.assert_called_once_with("tpch", 1)
        self.assertEqual(len(connection.scripts), 1)
        self.assertIn("CREATE TABLE IF NOT EXISTS nation", connection.scripts[0])
        self.assertEqual(
            len([sql for sql in connection.statements if sql.startswith("COPY INTO")]),
            len(hooks.TPCH_TABLES),
        )
        self.assertEqual(
            len([sql for sql in connection.statements if sql.startswith("ANALYZE TABLE")]),
            len(hooks.TPCH_TABLES),
        )

    def test_prepare_tpcds_uses_python_generated_data(self):
        connection = FakeConnection([0])
        with mock.patch.object(
            hooks, "generate_duckdb_data", return_value=Path("/tmp/tpcds_1")
        ) as generate:
            hooks.prepare_tpcds(connection)

        generate.assert_called_once_with("tpcds", 1)
        self.assertEqual(len(connection.scripts), 1)
        self.assertIn("create table tpcds.call_center", connection.scripts[0].lower())
        self.assertEqual(
            len([sql for sql in connection.statements if sql.startswith("COPY INTO")]),
            len(hooks.TPCDS_TABLES),
        )

    def test_prepare_stage_maps_environment_to_sql(self):
        connection = FakeConnection()
        environment = {
            "TEST_STAGE_STORAGE": "s3",
            "TEST_STAGE_DEDUP": "sub_path",
            "TEST_STAGE_SIZE": "large",
        }
        with mock.patch.dict(os.environ, environment, clear=False):
            hooks.prepare_stage(connection)

        sql = "\n".join(connection.statements)
        self.assertIn("CREATE OR REPLACE STAGE data URL = 's3://testbucket/data/'", sql)
        self.assertIn("SET GLOBAL copy_dedup_full_path_by_default = 0", sql)
        self.assertIn("SET GLOBAL parquet_fast_read_bytes = 0", sql)
        self.assertEqual(len(connection.scripts), 1)
        self.assertIn("CREATE TABLE ontime", connection.scripts[0])

    def test_execute_script_sends_one_statement_at_a_time(self):
        connection = hooks.DatabendConnection.__new__(hooks.DatabendConnection)
        raw_connection = mock.Mock()
        connection.connection = raw_connection

        connection.execute_script("create table a(a int); ; create table b(b int);\n")

        self.assertEqual(
            raw_connection.exec.call_args_list,
            [
                mock.call("create table a(a int)"),
                mock.call("create table b(b int)"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
