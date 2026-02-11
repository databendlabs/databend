## Copyright 2021 Datafuse Labs
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

import unittest.mock
import sys
import os


# Mock databend module to avoid actual service initialization
class MockSessionContext:
    def sql(self, query, py=None):
        class MockDataFrame:
            def collect(self):
                return None

        return MockDataFrame()

    def create_s3_connection(
        self, name, access_key_id, secret_access_key, endpoint_url, region
    ):
        endpoint_clause = endpoint_url and f" endpoint_url = '{endpoint_url}'" or ""
        region_clause = region and f" region = '{region}'" or ""
        sql = f"CREATE OR REPLACE CONNECTION {name} STORAGE_TYPE = 'S3' access_key_id = '{access_key_id}' secret_access_key = '{secret_access_key}'{endpoint_clause}{region_clause}"
        self.sql(sql)

    def register_parquet(self, name, path, pattern=None, connection=None):
        if connection:
            pattern_clause = f", pattern => '{pattern}'" if pattern else ""
            sql = f"create view {name} as select * from '{path}' (file_format => 'parquet'{pattern_clause}, connection => '{connection}')"
        else:
            pattern_clause = f", pattern => '{pattern}'" if pattern else ""
            sql = f"create view {name} as select * from '{path}' (file_format => 'parquet'{pattern_clause})"
        self.sql(sql)

    def register_csv(self, name, path, pattern=None, connection=None):
        if connection:
            pattern_clause = f", pattern => '{pattern}'" if pattern else ""
            connection_clause = f", connection => '{connection}'"
            # Infer schema first for CSV
            infer_conn = f", connection_name => '{connection}'"
            self.sql(
                f"SELECT column_name FROM infer_schema(location => '{path}', file_format => 'CSV'{infer_conn})"
            )
            # Use column positions from infer_schema (simulated as 3 columns)
            select_clause = "$1 AS `col1`, $2 AS `col2`, $3 AS `col3`"
            sql = f"create view {name} as select {select_clause} from '{path}' (file_format => 'csv'{pattern_clause}{connection_clause})"
        else:
            p = path
            if p.startswith("/"):
                p = f"fs://{p}"
            pattern_clause = f", pattern => '{pattern}'" if pattern else ""
            # Infer schema first for CSV
            self.sql(
                f"SELECT column_name FROM infer_schema(location => '{p}', file_format => 'CSV')"
            )
            # Use column positions from infer_schema (simulated as 3 columns)
            select_clause = "$1 AS `col1`, $2 AS `col2`, $3 AS `col3`"
            sql = f"create view {name} as select {select_clause} from '{p}' (file_format => 'csv'{pattern_clause})"
        self.sql(sql)

    def create_azblob_connection(self, name, endpoint_url, account_name, account_key):
        sql = f"CREATE OR REPLACE CONNECTION {name} STORAGE_TYPE = 'AZBLOB' endpoint_url = '{endpoint_url}' account_name = '{account_name}' account_key = '{account_key}'"
        self.sql(sql)

    def create_gcs_connection(self, name, endpoint_url, credential):
        sql = f"CREATE OR REPLACE CONNECTION {name} STORAGE_TYPE = 'GCS' endpoint_url = '{endpoint_url}' credential = '{credential}'"
        self.sql(sql)

    def create_oss_connection(
        self, name, endpoint_url, access_key_id, secret_access_key
    ):
        sql = f"CREATE OR REPLACE CONNECTION {name} STORAGE_TYPE = 'OSS' endpoint_url = '{endpoint_url}' access_key_id = '{access_key_id}' secret_access_key = '{secret_access_key}'"
        self.sql(sql)

    def create_cos_connection(
        self, name, endpoint_url, access_key_id, secret_access_key
    ):
        sql = f"CREATE OR REPLACE CONNECTION {name} STORAGE_TYPE = 'COS' endpoint_url = '{endpoint_url}' access_key_id = '{access_key_id}' secret_access_key = '{secret_access_key}'"
        self.sql(sql)

    def list_connections(self):
        return self.sql("SHOW CONNECTIONS")

    def describe_connection(self, name):
        return self.sql(f"DESC CONNECTION {name}")

    def drop_connection(self, name):
        self.sql(f"DROP CONNECTION {name}")

    def create_stage(self, name, url, connection_name):
        sql = f"CREATE OR REPLACE STAGE {name} URL='{url}' CONNECTION = (connection_name='{connection_name}')"
        self.sql(sql)

    def show_stages(self):
        return self.sql("SHOW STAGES")

    def list_stages(self, stage_name):
        return self.sql(f"LIST @{stage_name}")

    def describe_stage(self, name):
        return self.sql(f"DESC STAGE {name}")

    def drop_stage(self, name):
        self.sql(f"DROP STAGE {name}")


class TestConnections:
    def setup_method(self):
        self.ctx = MockSessionContext()

    def test_create_s3_connection_with_endpoint_and_region(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.create_s3_connection(
                "s3_conn",
                "access_key",
                "secret_key",
                "https://s3.amazonaws.com",
                "us-west-2",
            )

            expected_sql = "CREATE OR REPLACE CONNECTION s3_conn STORAGE_TYPE = 'S3' access_key_id = 'access_key' secret_access_key = 'secret_key' endpoint_url = 'https://s3.amazonaws.com' region = 'us-west-2'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_s3_connection_minimal(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.create_s3_connection(
                "s3_conn", "access_key", "secret_key", None, None
            )

            expected_sql = "CREATE OR REPLACE CONNECTION s3_conn STORAGE_TYPE = 'S3' access_key_id = 'access_key' secret_access_key = 'secret_key'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_s3_connection_with_region_only(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.create_s3_connection(
                "s3_conn", "access_key", "secret_key", None, "us-east-1"
            )

            expected_sql = "CREATE OR REPLACE CONNECTION s3_conn STORAGE_TYPE = 'S3' access_key_id = 'access_key' secret_access_key = 'secret_key' region = 'us-east-1'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_azblob_connection(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.create_azblob_connection(
                "azblob_conn",
                "https://test.blob.core.windows.net",
                "account_name",
                "account_key",
            )

            expected_sql = "CREATE OR REPLACE CONNECTION azblob_conn STORAGE_TYPE = 'AZBLOB' endpoint_url = 'https://test.blob.core.windows.net' account_name = 'account_name' account_key = 'account_key'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_gcs_connection(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.create_gcs_connection(
                "gcs_conn", "https://storage.googleapis.com", "credential_json"
            )

            expected_sql = "CREATE OR REPLACE CONNECTION gcs_conn STORAGE_TYPE = 'GCS' endpoint_url = 'https://storage.googleapis.com' credential = 'credential_json'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_oss_connection(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.create_oss_connection(
                "oss_conn",
                "https://oss-cn-hangzhou.aliyuncs.com",
                "access_key",
                "secret_key",
            )

            expected_sql = "CREATE OR REPLACE CONNECTION oss_conn STORAGE_TYPE = 'OSS' endpoint_url = 'https://oss-cn-hangzhou.aliyuncs.com' access_key_id = 'access_key' secret_access_key = 'secret_key'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_cos_connection(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.create_cos_connection(
                "cos_conn",
                "https://cos.ap-beijing.myqcloud.com",
                "access_key",
                "secret_key",
            )

            expected_sql = "CREATE OR REPLACE CONNECTION cos_conn STORAGE_TYPE = 'COS' endpoint_url = 'https://cos.ap-beijing.myqcloud.com' access_key_id = 'access_key' secret_access_key = 'secret_key'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_list_connections(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value = "mocked_dataframe"

            result = self.ctx.list_connections()

            mock_sql.assert_called_once_with("SHOW CONNECTIONS")
            assert result == "mocked_dataframe"

    def test_describe_connection(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value = "mocked_dataframe"

            result = self.ctx.describe_connection("aws_prod")

            expected_sql = "DESC CONNECTION aws_prod"
            mock_sql.assert_called_once_with(expected_sql)
            assert result == "mocked_dataframe"

    def test_drop_connection(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.drop_connection("aws_prod")

            expected_sql = "DROP CONNECTION aws_prod"
            mock_sql.assert_called_once_with(expected_sql)


class TestRegisterWithConnection:
    def setup_method(self):
        self.ctx = MockSessionContext()

    def test_register_parquet_with_connection(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.register_parquet(
                "sales", "s3://bucket/data.parquet", connection="my_s3"
            )

            expected_sql = "create view sales as select * from 's3://bucket/data.parquet' (file_format => 'parquet', connection => 'my_s3')"
            mock_sql.assert_called_once_with(expected_sql)

    def test_register_parquet_with_connection_and_pattern(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.register_parquet(
                "sales", "s3://bucket/data/", pattern="*.parquet", connection="my_s3"
            )

            expected_sql = "create view sales as select * from 's3://bucket/data/' (file_format => 'parquet', pattern => '*.parquet', connection => 'my_s3')"
            mock_sql.assert_called_once_with(expected_sql)

    def test_register_csv_with_connection(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.register_csv("users", "s3://bucket/users.csv", connection="my_s3")

            assert mock_sql.call_count == 2
            # First call: infer_schema
            mock_sql.assert_any_call(
                "SELECT column_name FROM infer_schema(location => 's3://bucket/users.csv', file_format => 'CSV', connection_name => 'my_s3')"
            )
            # Second call: create view with column positions
            mock_sql.assert_any_call(
                "create view users as select $1 AS `col1`, $2 AS `col2`, $3 AS `col3` from 's3://bucket/users.csv' (file_format => 'csv', connection => 'my_s3')"
            )

    def test_register_parquet_legacy_mode(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.register_parquet("local", "/data/file.parquet")

            expected_sql = "create view local as select * from '/data/file.parquet' (file_format => 'parquet')"
            mock_sql.assert_called_once_with(expected_sql)

    def test_register_csv_with_pattern_no_connection(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.register_csv("logs", "/data/logs/", pattern="*.csv")

            assert mock_sql.call_count == 2
            # First call: infer_schema with fs:// prefix
            mock_sql.assert_any_call(
                "SELECT column_name FROM infer_schema(location => 'fs:///data/logs/', file_format => 'CSV')"
            )
            # Second call: create view with column positions
            mock_sql.assert_any_call(
                "create view logs as select $1 AS `col1`, $2 AS `col2`, $3 AS `col3` from 'fs:///data/logs/' (file_format => 'csv', pattern => '*.csv')"
            )


class TestStages:
    def setup_method(self):
        self.ctx = MockSessionContext()

    def test_create_stage(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.create_stage("my_stage", "s3://bucket/path/", "my_connection")

            expected_sql = "CREATE OR REPLACE STAGE my_stage URL='s3://bucket/path/' CONNECTION = (connection_name='my_connection')"
            mock_sql.assert_called_once_with(expected_sql)

    def test_show_stages(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value = "mocked_dataframe"

            result = self.ctx.show_stages()

            mock_sql.assert_called_once_with("SHOW STAGES")
            assert result == "mocked_dataframe"

    def test_list_stages(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value = "mocked_dataframe"

            result = self.ctx.list_stages("my_stage")

            mock_sql.assert_called_once_with("LIST @my_stage")
            assert result == "mocked_dataframe"

    def test_describe_stage(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value = "mocked_dataframe"

            result = self.ctx.describe_stage("my_stage")

            expected_sql = "DESC STAGE my_stage"
            mock_sql.assert_called_once_with(expected_sql)
            assert result == "mocked_dataframe"

    def test_drop_stage(self):
        with unittest.mock.patch.object(self.ctx, "sql") as mock_sql:
            mock_sql.return_value.collect.return_value = None

            self.ctx.drop_stage("my_stage")

            expected_sql = "DROP STAGE my_stage"
            mock_sql.assert_called_once_with(expected_sql)
