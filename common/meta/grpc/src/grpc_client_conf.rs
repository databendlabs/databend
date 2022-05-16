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

use common_configs::MetaConfig;
use common_grpc::RpcClientConf;
use common_grpc::RpcClientTlsConfig;

#[derive(Clone, Debug, Default)]
pub struct MetaGrpcClientConf {
    pub meta_service_config: RpcClientConf,
    pub kv_service_config: RpcClientConf,
    pub client_timeout_in_second: u64,
}

impl From<&MetaConfig> for MetaGrpcClientConf {
    fn from(mc: &MetaConfig) -> Self {
        let meta_config = RpcClientConf {
            address: mc.address.clone(),
            endpoints: mc.endpoints.clone(),
            username: mc.username.clone(),
            password: mc.password.clone(),
            tls_conf: if mc.is_tls_enabled() {
                Some(RpcClientTlsConfig::from(mc))
            } else {
                None
            },
        };

        MetaGrpcClientConf {
            meta_service_config: meta_config.clone(),
            kv_service_config: meta_config,
            client_timeout_in_second: mc.client_timeout_in_second,
        }
    }
}
