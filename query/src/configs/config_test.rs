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

use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::configs::Config;
use crate::configs::LogConfig;
use crate::configs::MetaConfig;
use crate::configs::QueryConfig;
use crate::configs::StorageConfig;

// Default.
#[test]
fn test_default_config() -> Result<()> {
    let expect = Config {
        log: LogConfig::default(),
        meta: MetaConfig::default(),
        storage: StorageConfig::default(),
        query: QueryConfig::default(),
        config_file: "".to_string(),
    };
    let actual = Config::default();
    assert_eq!(actual, expect);

    let tom_expect = "config_file = \"\"

[query]
tenant = \"\"
namespace = \"\"
num_cpus = 8
mysql_handler_host = \"127.0.0.1\"
mysql_handler_port = 3307
max_active_sessions = 256
clickhouse_handler_host = \"127.0.0.1\"
clickhouse_handler_port = 9000
flight_api_address = \"127.0.0.1:9090\"
http_api_address = \"127.0.0.1:8080\"
metric_api_address = \"127.0.0.1:7070\"
api_tls_server_cert = \"\"
api_tls_server_key = \"\"
api_tls_server_root_ca_cert = \"\"
rpc_tls_server_cert = \"\"
rpc_tls_server_key = \"\"
rpc_tls_query_server_root_ca_cert = \"\"
rpc_tls_query_service_domain_name = \"localhost\"

[log]
log_level = \"INFO\"
log_dir = \"./_logs\"

[meta]
meta_address = \"\"
meta_username = \"root\"
meta_password = \"\"
rpc_tls_meta_server_root_ca_cert = \"\"
rpc_tls_meta_service_domain_name = \"localhost\"

[storage]
default_storage = \"disk\"

[storage.dfs]
address = \"\"
username = \"\"
password = \"\"
rpc_tls_storage_server_root_ca_cert = \"\"
rpc_tls_storage_service_domain_name = \"\"

[storage.disk]
data_path = \"\"

[storage.s3]
region = \"\"
key = \"\"
secret = \"\"
bucket = \"\"
";

    let tom_actual = toml::to_string(&actual).unwrap();
    assert_eq!(tom_actual, tom_expect);
    Ok(())
}

// From env, defaulting.
#[test]
fn test_env_config() -> Result<()> {
    std::env::set_var("LOG_LEVEL", "DEBUG");
    std::env::set_var("QUERY_TENANT", "tenant-1");
    std::env::set_var("QUERY_NAMESPACE", "cluster-1");
    std::env::set_var("QUERY_MYSQL_HANDLER_HOST", "0.0.0.0");
    std::env::set_var("QUERY_MYSQL_HANDLER_PORT", "3306");
    std::env::set_var("QUERY_MAX_ACTIVE_SESSIONS", "255");
    std::env::set_var("QUERY_CLICKHOUSE_HANDLER_HOST", "1.2.3.4");
    std::env::set_var("QUERY_CLICKHOUSE_HANDLER_PORT", "9000");
    std::env::set_var("QUERY_FLIGHT_API_ADDRESS", "1.2.3.4:9091");
    std::env::set_var("QUERY_HTTP_API_ADDRESS", "1.2.3.4:8081");
    std::env::set_var("QUERY_METRIC_API_ADDRESS", "1.2.3.4:7071");
    std::env::set_var("DFS_STORAGE_ADDRESS", "1.2.3.4:1234");
    std::env::set_var("DFS_STORAGE_USERNAME", "admin");
    std::env::set_var("DFS_STORAGE_PASSWORD", "password!");
    std::env::set_var("DISK_STORAGE_DATA_PATH", "/tmp/test");
    std::env::set_var("S3_STORAGE_REGION", "us.region");
    std::env::set_var("S3_STORAGE_KEY", "us.key");
    std::env::set_var("S3_STORAGE_SECRET", "us.secret");
    std::env::set_var("S3_STORAGE_BUCKET", "us.bucket");
    std::env::remove_var("CONFIG_FILE");

    let default = Config::default();
    let configured = Config::load_from_env(&default)?;
    assert_eq!("DEBUG", configured.log.log_level);

    assert_eq!("tenant-1", configured.query.tenant);
    assert_eq!("cluster-1", configured.query.namespace);
    assert_eq!("0.0.0.0", configured.query.mysql_handler_host);
    assert_eq!(3306, configured.query.mysql_handler_port);
    assert_eq!(255, configured.query.max_active_sessions);
    assert_eq!("1.2.3.4", configured.query.clickhouse_handler_host);
    assert_eq!(9000, configured.query.clickhouse_handler_port);

    assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
    assert_eq!("1.2.3.4:8081", configured.query.http_api_address);
    assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

    assert_eq!("1.2.3.4:1234", configured.storage.dfs.address);
    assert_eq!("admin", configured.storage.dfs.username);
    assert_eq!("password!", configured.storage.dfs.password);

    assert_eq!("/tmp/test", configured.storage.disk.data_path);

    assert_eq!("us.region", configured.storage.s3.region);
    assert_eq!("us.key", configured.storage.s3.key);
    assert_eq!("us.secret", configured.storage.s3.secret);
    assert_eq!("us.bucket", configured.storage.s3.bucket);

    // clean up
    std::env::remove_var("LOG_LEVEL");
    std::env::remove_var("QUERY_TENANT");
    std::env::remove_var("QUERY_NAMESPACE");
    std::env::remove_var("QUERY_MYSQL_HANDLER_HOST");
    std::env::remove_var("QUERY_MYSQL_HANDLER_PORT");
    std::env::remove_var("QUERY_MAX_ACTIVE_SESSIONS");
    std::env::remove_var("QUERY_CLICKHOUSE_HANDLER_HOST");
    std::env::remove_var("QUERY_CLICKHOUSE_HANDLER_PORT");
    std::env::remove_var("QUERY_CLICKHOUSE_HANDLER_THREAD_NUM");
    std::env::remove_var("QUERY_FLIGHT_API_ADDRESS");
    std::env::remove_var("QUERY_HTTP_API_ADDRESS");
    std::env::remove_var("QUERY_METRIC_API_ADDRESS");
    std::env::remove_var("DFS_STORAGE_ADDRESS");
    std::env::remove_var("DFS_STORAGE_USERNAME");
    std::env::remove_var("DFS_STORAGE_PASSWORD");
    std::env::remove_var("DISK_STORAGE_DATA_PATH");
    std::env::remove_var("S3_STORAGE_REGION");
    std::env::remove_var("S3_STORAGE_KEY");
    std::env::remove_var("S3_STORAGE_SECRET");
    std::env::remove_var("S3_STORAGE_BUCKET");
    Ok(())
}

#[test]
fn test_fuse_commit_version() -> Result<()> {
    let v = &crate::configs::config::DATABEND_COMMIT_VERSION;
    assert!(v.len() > 0);
    Ok(())
}
