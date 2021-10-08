// Copyright 2020 Datafuse Labs.
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

use structopt::StructOpt;
use structopt_toml::StructOptToml;

use crate::configs::Config;

// Query env.
const QUERY_TENANT: &str = "QUERY_TENANT";
const QUERY_NAMESPACE: &str = "QUERY_NAMESPACE";
const QUERY_NUM_CPUS: &str = "QUERY_NUM_CPUS";
const QUERY_MYSQL_HANDLER_HOST: &str = "QUERY_MYSQL_HANDLER_HOST";
const QUERY_MYSQL_HANDLER_PORT: &str = "QUERY_MYSQL_HANDLER_PORT";
const QUERY_MAX_ACTIVE_SESSIONS: &str = "QUERY_MAX_ACTIVE_SESSIONS";
const QUERY_CLICKHOUSE_HANDLER_HOST: &str = "QUERY_CLICKHOUSE_HANDLER_HOST";
const QUERY_CLICKHOUSE_HANDLER_PORT: &str = "QUERY_CLICKHOUSE_HANDLER_PORT";
const QUERY_FLIGHT_API_ADDRESS: &str = "QUERY_FLIGHT_API_ADDRESS";
const QUERY_HTTP_API_ADDRESS: &str = "QUERY_HTTP_API_ADDRESS";
const QUERY_METRICS_API_ADDRESS: &str = "QUERY_METRIC_API_ADDRESS";
const QUERY_API_TLS_SERVER_CERT: &str = "QUERY_API_TLS_SERVER_CERT";
const QUERY_API_TLS_SERVER_KEY: &str = "QUERY_API_TLS_SERVER_KEY";
const QUERY_API_TLS_SERVER_ROOT_CA_CERT: &str = "QUERY_API_TLS_SERVER_ROOT_CA_CERT";

const QUERY_RPC_TLS_SERVER_CERT: &str = "QUERY_RPC_TLS_SERVER_CERT";
const QUERY_RPC_TLS_SERVER_KEY: &str = "QUERY_RPC_TLS_SERVER_KEY";
const QUERY_RPC_TLS_SERVER_ROOT_CA_CERT: &str = "QUERY_RPC_TLS_SERVER_ROOT_CA_CERT";
const QUERY_RPC_TLS_SERVICE_DOMAIN_NAME: &str = "QUERY_RPC_TLS_SERVICE_DOMAIN_NAME";

/// Query config group.
/// serde(default) make the toml de to default working.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
pub struct QueryConfig {
    #[structopt(long, env = QUERY_TENANT, default_value = "", help = "Tenant id for get the information from the MetaStore")]
    #[serde(default)]
    pub tenant: String,

    #[structopt(long, env = QUERY_NAMESPACE, default_value = "", help = "Namespace for construct the cluster")]
    #[serde(default)]
    pub namespace: String,

    #[structopt(long, env = QUERY_NUM_CPUS, default_value = "0")]
    #[serde(default)]
    pub num_cpus: u64,

    #[structopt(
    long,
    env = QUERY_MYSQL_HANDLER_HOST,
    default_value = "127.0.0.1"
    )]
    #[serde(default)]
    pub mysql_handler_host: String,

    #[structopt(long, env = QUERY_MYSQL_HANDLER_PORT, default_value = "3307")]
    #[serde(default)]
    pub mysql_handler_port: u16,

    #[structopt(
    long,
    env = QUERY_MAX_ACTIVE_SESSIONS,
    default_value = "256"
    )]
    #[serde(default)]
    pub max_active_sessions: u64,

    #[structopt(
    long,
    env = QUERY_CLICKHOUSE_HANDLER_HOST,
    default_value = "127.0.0.1"
    )]
    #[serde(default)]
    pub clickhouse_handler_host: String,

    #[structopt(
    long,
    env = QUERY_CLICKHOUSE_HANDLER_PORT,
    default_value = "9000"
    )]
    #[serde(default)]
    pub clickhouse_handler_port: u16,

    #[structopt(
    long,
    env = QUERY_FLIGHT_API_ADDRESS,
    default_value = "127.0.0.1:9090"
    )]
    #[serde(default)]
    pub flight_api_address: String,

    #[structopt(
    long,
    env = QUERY_HTTP_API_ADDRESS,
    default_value = "127.0.0.1:8080"
    )]
    #[serde(default)]
    pub http_api_address: String,

    #[structopt(
    long,
    env = QUERY_METRICS_API_ADDRESS,
    default_value = "127.0.0.1:7070"
    )]
    #[serde(default)]
    pub metric_api_address: String,

    #[structopt(long, env = QUERY_API_TLS_SERVER_CERT, default_value = "")]
    #[serde(default)]
    pub api_tls_server_cert: String,

    #[structopt(long, env = QUERY_API_TLS_SERVER_KEY, default_value = "")]
    #[serde(default)]
    pub api_tls_server_key: String,

    #[structopt(long, env = QUERY_API_TLS_SERVER_ROOT_CA_CERT, default_value = "")]
    #[serde(default)]
    pub api_tls_server_root_ca_cert: String,

    #[structopt(
        long,
        env = "QUERY_RPC_TLS_SERVER_CERT",
        default_value = "",
        help = "rpc server cert"
    )]
    #[serde(default)]
    pub rpc_tls_server_cert: String,

    #[structopt(
        long,
        env = "QUERY_RPC_TLS_SERVER_KEY",
        default_value = "key for rpc server cert"
    )]
    #[serde(default)]
    pub rpc_tls_server_key: String,

    #[structopt(
        long,
        env = "QUERY_RPC_TLS_SERVER_ROOT_CA_CERT",
        default_value = "",
        help = "Certificate for client to identify query rpc server"
    )]
    #[serde(default)]
    pub rpc_tls_query_server_root_ca_cert: String,

    #[structopt(
        long,
        env = "QUERY_RPC_TLS_SERVICE_DOMAIN_NAME",
        default_value = "localhost"
    )]
    #[serde(default)]
    pub rpc_tls_query_service_domain_name: String,
}

impl QueryConfig {
    pub fn default() -> Self {
        QueryConfig {
            tenant: "".to_string(),
            namespace: "".to_string(),
            num_cpus: 8,
            mysql_handler_host: "127.0.0.1".to_string(),
            mysql_handler_port: 3307,
            max_active_sessions: 256,
            clickhouse_handler_host: "127.0.0.1".to_string(),
            clickhouse_handler_port: 9000,
            flight_api_address: "127.0.0.1:9090".to_string(),
            http_api_address: "127.0.0.1:8080".to_string(),
            metric_api_address: "127.0.0.1:7070".to_string(),
            api_tls_server_cert: "".to_string(),
            api_tls_server_key: "".to_string(),
            api_tls_server_root_ca_cert: "".to_string(),
            rpc_tls_server_cert: "".to_string(),
            rpc_tls_server_key: "".to_string(),
            rpc_tls_query_server_root_ca_cert: "".to_string(),
            rpc_tls_query_service_domain_name: "localhost".to_string(),
        }
    }

    pub fn load_from_env(mut_config: &mut Config) {
        env_helper!(mut_config, query, tenant, String, QUERY_TENANT);
        env_helper!(mut_config, query, namespace, String, QUERY_NAMESPACE);
        env_helper!(mut_config, query, num_cpus, u64, QUERY_NUM_CPUS);
        env_helper!(
            mut_config,
            query,
            mysql_handler_host,
            String,
            QUERY_MYSQL_HANDLER_HOST
        );
        env_helper!(
            mut_config,
            query,
            mysql_handler_port,
            u16,
            QUERY_MYSQL_HANDLER_PORT
        );
        env_helper!(
            mut_config,
            query,
            max_active_sessions,
            u64,
            QUERY_MAX_ACTIVE_SESSIONS
        );
        env_helper!(
            mut_config,
            query,
            clickhouse_handler_host,
            String,
            QUERY_CLICKHOUSE_HANDLER_HOST
        );
        env_helper!(
            mut_config,
            query,
            clickhouse_handler_port,
            u16,
            QUERY_CLICKHOUSE_HANDLER_PORT
        );
        env_helper!(
            mut_config,
            query,
            flight_api_address,
            String,
            QUERY_FLIGHT_API_ADDRESS
        );
        env_helper!(
            mut_config,
            query,
            http_api_address,
            String,
            QUERY_HTTP_API_ADDRESS
        );
        env_helper!(
            mut_config,
            query,
            metric_api_address,
            String,
            QUERY_METRICS_API_ADDRESS
        );

        // for api http service
        env_helper!(
            mut_config,
            query,
            api_tls_server_cert,
            String,
            QUERY_API_TLS_SERVER_CERT
        );

        env_helper!(
            mut_config,
            query,
            api_tls_server_key,
            String,
            QUERY_API_TLS_SERVER_KEY
        );

        // for query rpc server
        env_helper!(
            mut_config,
            query,
            rpc_tls_server_cert,
            String,
            QUERY_RPC_TLS_SERVER_CERT
        );

        env_helper!(
            mut_config,
            query,
            rpc_tls_server_key,
            String,
            QUERY_RPC_TLS_SERVER_KEY
        );

        // for query rpc client
        env_helper!(
            mut_config,
            query,
            rpc_tls_query_server_root_ca_cert,
            String,
            QUERY_RPC_TLS_SERVER_ROOT_CA_CERT
        );
        env_helper!(
            mut_config,
            query,
            rpc_tls_query_service_domain_name,
            String,
            QUERY_RPC_TLS_SERVICE_DOMAIN_NAME
        );
    }
}
