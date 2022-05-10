//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_configs::MetaConfig;
use common_configs::QueryConfig;

#[derive(Clone, Debug, Default)]
pub struct RpcClientTlsConfig {
    pub rpc_tls_server_root_ca_cert: String,
    pub domain_name: String,
}

impl RpcClientTlsConfig {
    pub fn enabled(&self) -> bool {
        !self.rpc_tls_server_root_ca_cert.is_empty() && !self.domain_name.is_empty()
    }
}

impl From<&QueryConfig> for RpcClientTlsConfig {
    fn from(qc: &QueryConfig) -> Self {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: qc.rpc_tls_query_server_root_ca_cert.to_string(),
            domain_name: qc.rpc_tls_query_service_domain_name.to_string(),
        }
    }
}

impl From<&MetaConfig> for RpcClientTlsConfig {
    fn from(mc: &MetaConfig) -> Self {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: mc.rpc_tls_meta_server_root_ca_cert.to_string(),
            domain_name: mc.rpc_tls_meta_service_domain_name.to_string(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RpcClientConf {
    pub address: String,
    pub username: String,
    pub password: String,
    pub tls_conf: Option<RpcClientTlsConfig>,
}

impl RpcClientConf {
    pub fn local_mode(&self) -> bool {
        self.address.is_empty()
    }
}
