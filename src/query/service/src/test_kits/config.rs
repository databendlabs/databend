// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_base::base::GlobalUniqName;
use databend_common_config::BuiltInConfig;
use databend_common_config::InnerConfig;
use databend_common_config::UDFConfig;
use databend_common_config::UserAuthConfig;
use databend_common_config::UserConfig;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use tempfile::TempDir;

pub struct ConfigBuilder {
    conf: InnerConfig,
}

impl ConfigBuilder {
    pub fn create() -> ConfigBuilder {
        let mut conf = InnerConfig::default();
        conf.query.tenant_id = Tenant::new_literal("test");
        conf.log = databend_common_tracing::Config::new_testing();
        conf.query.cluster_id = String::from("test_cluster");

        // add builtin users for test
        let users = vec![UserConfig {
            name: "root".to_string(),
            auth: UserAuthConfig {
                auth_type: "no_password".to_string(),
                auth_string: None,
            },
        }];

        // add builtin udfs for test
        let udfs = vec![UDFConfig {
            name: "test_builtin_ping".to_string(),
            definition: "CREATE OR REPLACE FUNCTION test_builtin_ping (STRING)
    RETURNS STRING
    LANGUAGE python
HANDLER = 'ping'
ADDRESS = 'https://databend.com';"
                .to_string(),
        }];
        conf.query.builtin = BuiltInConfig { users, udfs };

        // set node_id to a unique value
        conf.query.node_id = GlobalUniqName::unique();

        // set storage to fs
        let tmp_dir = TempDir::new().expect("create tmp dir failed");
        let root = tmp_dir.path().to_str().unwrap().to_string();
        conf.storage.params = StorageParams::Fs(StorageFsConfig { root });
        conf.storage.allow_insecure = true;

        ConfigBuilder { conf }
    }

    pub fn with_management_mode(&self) -> ConfigBuilder {
        let mut conf = self.conf.clone();
        conf.query.management_mode = true;
        ConfigBuilder { conf }
    }

    pub fn api_tls_server_key(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.api_tls_server_key = value.into();
        self
    }

    pub fn api_tls_server_cert(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.api_tls_server_cert = value.into();
        self
    }

    pub fn api_tls_server_root_ca_cert(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.api_tls_server_root_ca_cert = value.into();
        self
    }

    pub fn max_active_sessions(mut self, value: u64) -> ConfigBuilder {
        self.conf.query.max_active_sessions = value;
        self
    }

    pub fn parquet_fast_read_bytes(mut self, value: u64) -> ConfigBuilder {
        self.conf.query.parquet_fast_read_bytes = Some(value);
        self
    }

    pub fn max_storage_io_requests(mut self, value: u64) -> ConfigBuilder {
        self.conf.query.max_storage_io_requests = Some(value);
        self
    }

    pub fn jwt_key_file(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.jwt_key_file = value.into();
        self
    }

    pub fn add_user(mut self, _user_name: &str, user: UserConfig) -> ConfigBuilder {
        let users = vec![user];
        self.conf.query.builtin = BuiltInConfig {
            users,
            udfs: vec![],
        };
        self
    }

    pub fn http_handler_result_timeout(mut self, value: impl Into<u64>) -> ConfigBuilder {
        self.conf.query.http_handler_result_timeout_secs = value.into();
        self
    }

    pub fn http_handler_tls_server_key(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.http_handler_tls_server_key = value.into();
        self
    }

    pub fn http_handler_tls_server_cert(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.http_handler_tls_server_cert = value.into();
        self
    }

    pub fn http_handler_tls_server_root_ca_cert(
        mut self,
        value: impl Into<String>,
    ) -> ConfigBuilder {
        self.conf.query.http_handler_tls_server_root_ca_cert = value.into();
        self
    }

    pub fn rpc_tls_server_key(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.rpc_tls_server_key = value.into();
        self
    }

    pub fn rpc_tls_server_cert(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.rpc_tls_server_cert = value.into();
        self
    }

    pub fn query_flight_address(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.flight_api_address = value.into();
        self
    }

    pub fn enable_table_meta_cache(mut self) -> ConfigBuilder {
        self.conf.cache.enable_table_meta_cache = true;
        self
    }

    pub fn table_meta_segment_bytes(mut self, value: u64) -> ConfigBuilder {
        self.conf.cache.table_meta_segment_bytes = value;
        self
    }

    pub fn build(self) -> InnerConfig {
        self.conf
    }

    pub fn config(&self) -> InnerConfig {
        self.conf.clone()
    }
}
