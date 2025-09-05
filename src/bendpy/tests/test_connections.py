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
    
    def create_s3_connection(self, name, access_key_id, secret_access_key, endpoint_url, region):
        endpoint_clause = endpoint_url and f" endpoint_url = '{endpoint_url}'" or ""
        region_clause = region and f" region = '{region}'" or ""
        sql = f"CREATE OR REPLACE CONNECTION {name} TYPE = 'S3' access_key_id = '{access_key_id}' secret_access_key = '{secret_access_key}'{endpoint_clause}{region_clause}"
        self.sql(sql)
        
    def create_azblob_connection(self, name, endpoint_url, account_name, account_key):
        sql = f"CREATE OR REPLACE CONNECTION {name} TYPE = 'AZBLOB' endpoint_url = '{endpoint_url}' account_name = '{account_name}' account_key = '{account_key}'"
        self.sql(sql)
        
    def create_gcs_connection(self, name, endpoint_url, credential):
        sql = f"CREATE OR REPLACE CONNECTION {name} TYPE = 'GCS' endpoint_url = '{endpoint_url}' credential = '{credential}'"
        self.sql(sql)
        
    def create_oss_connection(self, name, endpoint_url, access_key_id, secret_access_key):
        sql = f"CREATE OR REPLACE CONNECTION {name} TYPE = 'OSS' endpoint_url = '{endpoint_url}' access_key_id = '{access_key_id}' secret_access_key = '{secret_access_key}'"
        self.sql(sql)
        
    def create_cos_connection(self, name, endpoint_url, access_key_id, secret_access_key):
        sql = f"CREATE OR REPLACE CONNECTION {name} TYPE = 'COS' endpoint_url = '{endpoint_url}' access_key_id = '{access_key_id}' secret_access_key = '{secret_access_key}'"
        self.sql(sql)
        
    def list_connections(self):
        return self.sql("SHOW CONNECTIONS")
        
    def describe_connection(self, name):
        return self.sql(f"DESC CONNECTION {name}")
        
    def drop_connection(self, name):
        self.sql(f"DROP CONNECTION {name}")


class TestConnections:
    def setup_method(self):
        self.ctx = MockSessionContext()

    def test_create_s3_connection_with_endpoint_and_region(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value.collect.return_value = None
            
            self.ctx.create_s3_connection(
                "s3_conn",
                "access_key",
                "secret_key",
                "https://s3.amazonaws.com",
                "us-west-2"
            )
            
            expected_sql = "CREATE OR REPLACE CONNECTION s3_conn TYPE = 'S3' access_key_id = 'access_key' secret_access_key = 'secret_key' endpoint_url = 'https://s3.amazonaws.com' region = 'us-west-2'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_s3_connection_minimal(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value.collect.return_value = None
            
            self.ctx.create_s3_connection(
                "s3_conn",
                "access_key",
                "secret_key",
                None,
                None
            )
            
            expected_sql = "CREATE OR REPLACE CONNECTION s3_conn TYPE = 'S3' access_key_id = 'access_key' secret_access_key = 'secret_key'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_s3_connection_with_region_only(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value.collect.return_value = None
            
            self.ctx.create_s3_connection(
                "s3_conn",
                "access_key", 
                "secret_key",
                None,
                "us-east-1"
            )
            
            expected_sql = "CREATE OR REPLACE CONNECTION s3_conn TYPE = 'S3' access_key_id = 'access_key' secret_access_key = 'secret_key' region = 'us-east-1'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_azblob_connection(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value.collect.return_value = None
            
            self.ctx.create_azblob_connection(
                "azblob_conn",
                "https://test.blob.core.windows.net",
                "account_name",
                "account_key"
            )
            
            expected_sql = "CREATE OR REPLACE CONNECTION azblob_conn TYPE = 'AZBLOB' endpoint_url = 'https://test.blob.core.windows.net' account_name = 'account_name' account_key = 'account_key'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_gcs_connection(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value.collect.return_value = None
            
            self.ctx.create_gcs_connection(
                "gcs_conn",
                "https://storage.googleapis.com",
                "credential_json"
            )
            
            expected_sql = "CREATE OR REPLACE CONNECTION gcs_conn TYPE = 'GCS' endpoint_url = 'https://storage.googleapis.com' credential = 'credential_json'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_oss_connection(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value.collect.return_value = None
            
            self.ctx.create_oss_connection(
                "oss_conn",
                "https://oss-cn-hangzhou.aliyuncs.com",
                "access_key",
                "secret_key"
            )
            
            expected_sql = "CREATE OR REPLACE CONNECTION oss_conn TYPE = 'OSS' endpoint_url = 'https://oss-cn-hangzhou.aliyuncs.com' access_key_id = 'access_key' secret_access_key = 'secret_key'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_create_cos_connection(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value.collect.return_value = None
            
            self.ctx.create_cos_connection(
                "cos_conn",
                "https://cos.ap-beijing.myqcloud.com",
                "access_key",
                "secret_key"
            )
            
            expected_sql = "CREATE OR REPLACE CONNECTION cos_conn TYPE = 'COS' endpoint_url = 'https://cos.ap-beijing.myqcloud.com' access_key_id = 'access_key' secret_access_key = 'secret_key'"
            mock_sql.assert_called_once_with(expected_sql)

    def test_list_connections(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value = "mocked_dataframe"
            
            result = self.ctx.list_connections()
            
            mock_sql.assert_called_once_with("SHOW CONNECTIONS")
            assert result == "mocked_dataframe"

    def test_describe_connection(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value = "mocked_dataframe"
            
            result = self.ctx.describe_connection("aws_prod")
            
            expected_sql = "DESC CONNECTION aws_prod"
            mock_sql.assert_called_once_with(expected_sql)
            assert result == "mocked_dataframe"

    def test_drop_connection(self):
        with unittest.mock.patch.object(self.ctx, 'sql') as mock_sql:
            mock_sql.return_value.collect.return_value = None
            
            self.ctx.drop_connection("aws_prod")
            
            expected_sql = "DROP CONNECTION aws_prod"
            mock_sql.assert_called_once_with(expected_sql)