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

use common_exception::Result;
use databend_query::configs::Config;
use databend_query::configs::LogConfig;
use databend_query::configs::MetaConfig;
use databend_query::configs::QueryConfig;
use databend_query::configs::StorageConfig;
use pretty_assertions::assert_eq;

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
tenant_id = \"\"
cluster_id = \"\"
num_cpus = 8
mysql_handler_host = \"127.0.0.1\"
mysql_handler_port = 3307
max_active_sessions = 256
clickhouse_handler_host = \"127.0.0.1\"
clickhouse_handler_port = 9000
http_handler_host = \"127.0.0.1\"
http_handler_port = 8000
http_handler_result_timeout_millis = 10000
flight_api_address = \"127.0.0.1:9090\"
http_api_address = \"127.0.0.1:8080\"
metric_api_address = \"127.0.0.1:7070\"
http_handler_tls_server_cert = \"\"
http_handler_tls_server_key = \"\"
http_handler_tls_server_root_ca_cert = \"\"
api_tls_server_cert = \"\"
api_tls_server_key = \"\"
api_tls_server_root_ca_cert = \"\"
rpc_tls_server_cert = \"\"
rpc_tls_server_key = \"\"
rpc_tls_query_server_root_ca_cert = \"\"
rpc_tls_query_service_domain_name = \"localhost\"
table_engine_csv_enabled = false
table_engine_parquet_enabled = false
table_engine_memory_enabled = true
database_engine_github_enabled = true
wait_timeout_mills = 5000
max_query_log_size = 10000
table_cache_enabled = false
table_cache_snapshot_count = 256
table_cache_segment_count = 10240
table_cache_block_meta_count = 102400
table_memory_cache_mb_size = 256
table_disk_cache_root = \"_cache\"
table_disk_cache_mb_size = 1024
management_mode = false
jwt_key_file = \"\"

[log]
log_level = \"INFO\"
log_dir = \"./_logs\"

[meta]
meta_embedded_dir = \"./_meta_embedded\"
meta_address = \"\"
meta_username = \"root\"
meta_password = \"\"
meta_client_timeout_in_second = 10
rpc_tls_meta_server_root_ca_cert = \"\"
rpc_tls_meta_service_domain_name = \"localhost\"

[storage]
storage_type = \"disk\"

[storage.disk]
data_path = \"_data\"
temp_data_path = \"\"

[storage.s3]
region = \"\"
endpoint_url = \"\"
access_key_id = \"\"
secret_access_key = \"\"
enable_pod_iam_policy = false
bucket = \"\"

[storage.azure_storage_blob]
account = \"\"
master_key = \"\"
container = \"\"
";

    let tom_actual = toml::to_string(&actual).unwrap();
    assert_eq!(tom_actual, tom_expect);
    Ok(())
}

// From env, defaulting.
#[test]
fn test_env_config() -> Result<()> {
    std::env::set_var("LOG_LEVEL", "DEBUG");
    std::env::set_var("QUERY_TENANT_ID", "tenant-1");
    std::env::set_var("QUERY_CLUSTER_ID", "cluster-1");
    std::env::set_var("QUERY_MYSQL_HANDLER_HOST", "127.0.0.1");
    std::env::set_var("QUERY_MYSQL_HANDLER_PORT", "3306");
    std::env::set_var("QUERY_MAX_ACTIVE_SESSIONS", "255");
    std::env::set_var("QUERY_CLICKHOUSE_HANDLER_HOST", "1.2.3.4");
    std::env::set_var("QUERY_CLICKHOUSE_HANDLER_PORT", "9000");
    std::env::set_var("QUERY_FLIGHT_API_ADDRESS", "1.2.3.4:9091");
    std::env::set_var("QUERY_HTTP_API_ADDRESS", "1.2.3.4:8081");
    std::env::set_var("QUERY_METRIC_API_ADDRESS", "1.2.3.4:7071");
    std::env::set_var("QUERY_TABLE_CACHE_ENABLED", "true");
    std::env::set_var("QUERY_TABLE_MEMORY_CACHE_MB_SIZE", "512");
    std::env::set_var("QUERY_TABLE_DISK_CACHE_ROOT", "_cache_env");
    std::env::set_var("QUERY_TABLE_DISK_CACHE_MB_SIZE", "512");
    std::env::set_var("STORAGE_TYPE", "s3");
    std::env::set_var("DISK_STORAGE_DATA_PATH", "/tmp/test");
    std::env::set_var("S3_STORAGE_REGION", "us.region");
    std::env::set_var("S3_STORAGE_ENDPOINT_URL", "");
    std::env::set_var("S3_STORAGE_ACCESS_KEY_ID", "us.key.id");
    std::env::set_var("S3_STORAGE_SECRET_ACCESS_KEY", "us.key");
    std::env::set_var("S3_STORAGE_ENABLE_POD_IAM_POLICY", "true");
    std::env::set_var("S3_STORAGE_BUCKET", "us.bucket");
    std::env::set_var("QUERY_TABLE_ENGINE_CSV_ENABLED", "true");
    std::env::set_var("QUERY_TABLE_ENGINE_PARQUET_ENABLED", "true");
    std::env::set_var("QUERY_TABLE_ENGINE_MEMORY_ENABLED", "true");
    std::env::set_var("QUERY_DATABASE_ENGINE_GITHUB_ENABLED", "false");
    std::env::remove_var("CONFIG_FILE");

    let default = Config::default();
    let configured = Config::load_from_env(&default)?;
    assert_eq!("DEBUG", configured.log.log_level);

    assert_eq!("tenant-1", configured.query.tenant_id);
    assert_eq!("cluster-1", configured.query.cluster_id);
    assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
    assert_eq!(3306, configured.query.mysql_handler_port);
    assert_eq!(255, configured.query.max_active_sessions);
    assert_eq!("1.2.3.4", configured.query.clickhouse_handler_host);
    assert_eq!(9000, configured.query.clickhouse_handler_port);

    assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
    assert_eq!("1.2.3.4:8081", configured.query.http_api_address);
    assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

    assert_eq!("s3", configured.storage.storage_type);

    assert_eq!("/tmp/test", configured.storage.disk.data_path);

    assert_eq!("us.region", configured.storage.s3.region);
    assert_eq!("", configured.storage.s3.endpoint_url);
    assert_eq!("us.key.id", configured.storage.s3.access_key_id);
    assert_eq!("us.key", configured.storage.s3.secret_access_key);
    assert!(configured.storage.s3.enable_pod_iam_policy);
    assert_eq!("us.bucket", configured.storage.s3.bucket);

    assert!(configured.query.table_engine_csv_enabled);
    assert!(configured.query.table_engine_parquet_enabled);
    assert!(configured.query.table_engine_memory_enabled);
    assert!(!configured.query.database_engine_github_enabled);

    assert!(configured.query.table_cache_enabled);
    assert_eq!(512, configured.query.table_memory_cache_mb_size);
    assert_eq!("_cache_env", configured.query.table_disk_cache_root);
    assert_eq!(512, configured.query.table_disk_cache_mb_size);

    // clean up
    std::env::remove_var("LOG_LEVEL");
    std::env::remove_var("QUERY_TENANT_ID");
    std::env::remove_var("QUERY_CLUSTER_ID");
    std::env::remove_var("QUERY_MYSQL_HANDLER_HOST");
    std::env::remove_var("QUERY_MYSQL_HANDLER_PORT");
    std::env::remove_var("QUERY_MAX_ACTIVE_SESSIONS");
    std::env::remove_var("QUERY_CLICKHOUSE_HANDLER_HOST");
    std::env::remove_var("QUERY_CLICKHOUSE_HANDLER_PORT");
    std::env::remove_var("QUERY_CLICKHOUSE_HANDLER_THREAD_NUM");
    std::env::remove_var("QUERY_FLIGHT_API_ADDRESS");
    std::env::remove_var("QUERY_HTTP_API_ADDRESS");
    std::env::remove_var("QUERY_METRIC_API_ADDRESS");
    std::env::remove_var("QUERY_TABLE_CACHE_ENABLED");
    std::env::remove_var("QUERY_TABLE_MEMORY_CACHE_MB_SIZE");
    std::env::remove_var("QUERY_TABLE_DISK_CACHE_ROOT");
    std::env::remove_var("QUERY_TABLE_DISK_CACHE_MB_SIZE");
    std::env::remove_var("STORAGE_TYPE");
    std::env::remove_var("DISK_STORAGE_DATA_PATH");
    std::env::remove_var("S3_STORAGE_REGION");
    std::env::remove_var("S3_STORAGE_ACCESS_KEY_ID");
    std::env::remove_var("S3_STORAGE_SECRET_ACCESS_KEY");
    std::env::remove_var("S3_STORAGE_BUCKET");
    std::env::remove_var("S3_STORAGE_ENABLE_POD_IAM_POLICY");
    std::env::remove_var("QUERY_TABLE_ENGINE_CSV_ENABLED");
    std::env::remove_var("QUERY_TABLE_ENGINE_PARQUET_ENABLED");
    std::env::remove_var("QUERY_TABLE_ENGINE_MEMORY_ENABLED");
    std::env::remove_var("QUERY_DATABASE_ENGINE_GITHUB_ENABLED");
    Ok(())
}

#[test]
fn test_fuse_commit_version() -> Result<()> {
    let v = &databend_query::configs::DATABEND_COMMIT_VERSION;
    assert!(v.len() > 0);
    Ok(())
}
