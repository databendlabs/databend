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

use common_store_api_sdk::ClientConf;
use common_store_api_sdk::RpcClientTlsConfig;
use common_store_api_sdk::StoreClientConf;

use crate::configs::Config;

// since `store-api-sdk` should not depends on query
// we provide a converter for it -- just copy things around
impl From<&Config> for StoreClientConf {
    fn from(conf: &Config) -> Self {
        let meta_tls_conf = if conf.tls_meta_cli_enabled() {
            Some(RpcClientTlsConfig {
                rpc_tls_server_root_ca_cert: conf.meta.rpc_tls_meta_server_root_ca_cert.clone(),
                domain_name: conf.meta.rpc_tls_meta_service_domain_name.clone(),
            })
        } else {
            None
        };

        let meta_config = ClientConf {
            address: conf.meta.meta_address.clone(),
            username: conf.meta.meta_username.clone(),
            password: conf.meta.meta_password.clone(),
            tls_conf: meta_tls_conf,
        };

        StoreClientConf {
            // kv service is configured by conf.meta
            kv_service_config: meta_config.clone(),
            // copy meta config from query config
            meta_service_config: meta_config,
        }
    }
}
