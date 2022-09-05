// Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;

use common_meta_types::AuthInfo;
use common_users::iam_config::IAMConfig;
use databend_query::Config;

pub struct ConfigBuilder {
    conf: Config,
}

impl ConfigBuilder {
    pub fn create() -> ConfigBuilder {
        let mut conf = Config::default();
        conf.query.tenant_id = "test".to_string();
        conf.log = common_tracing::Config::new_testing();

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

    pub fn jwt_key_file(mut self, value: impl Into<String>) -> ConfigBuilder {
        self.conf.query.jwt_key_file = value.into();
        self
    }

    pub fn add_user(mut self, user_name: &str, auth_info: AuthInfo) -> ConfigBuilder {
        let mut users = HashMap::new();
        users.insert(user_name.to_string(), auth_info);
        self.conf.iam = IAMConfig { users };
        self
    }

    pub fn async_insert_busy_timeout(mut self, value: u64) -> ConfigBuilder {
        self.conf.query.async_insert_busy_timeout = value;
        self
    }

    pub fn async_insert_max_data_size(mut self, value: u64) -> ConfigBuilder {
        self.conf.query.async_insert_max_data_size = value;
        self
    }

    pub fn async_insert_stale_timeout(mut self, value: u64) -> ConfigBuilder {
        self.conf.query.async_insert_stale_timeout = value;
        self
    }

    pub fn http_handler_result_time_out(mut self, value: impl Into<u64>) -> ConfigBuilder {
        self.conf.query.http_handler_result_timeout_millis = value.into();
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

    pub fn build(self) -> Config {
        self.conf
    }

    pub fn config(&self) -> Config {
        self.conf.clone()
    }
}
