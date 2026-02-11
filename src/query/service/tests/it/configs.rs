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

use std::collections::HashMap;
use std::env::temp_dir;
use std::fs;
use std::io::Write;

use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeConfig;
use databend_common_config::CatalogConfig;
use databend_common_config::CatalogHiveConfig;
use databend_common_config::InnerConfig;
use databend_common_config::ThriftProtocol;
use databend_common_exception::ErrorCode;
use pretty_assertions::assert_eq;

// From env, defaulting.
#[test]
fn test_env_config_s3() -> anyhow::Result<()> {
    temp_env::with_vars(
        vec![
            ("LOG_LEVEL", Some("DEBUG")),
            ("QUERY_TENANT_ID", Some("tenant-1")),
            ("QUERY_CLUSTER_ID", Some("cluster-1")),
            ("QUERY_MYSQL_HANDLER_HOST", Some("127.0.0.1")),
            ("QUERY_MYSQL_HANDLER_PORT", Some("3306")),
            ("QUERY_MAX_ACTIVE_SESSIONS", Some("255")),
            ("QUERY_CLICKHOUSE_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HANDLER_PORT", Some("9000")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_PORT", Some("8124")),
            ("QUERY_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_HTTP_HANDLER_PORT", Some("8001")),
            ("QUERY_FLIGHT_API_ADDRESS", Some("1.2.3.4:9091")),
            ("QUERY_ADMIN_API_ADDRESS", Some("1.2.3.4:8081")),
            ("QUERY_METRIC_API_ADDRESS", Some("1.2.3.4:7071")),
            ("CACHE_ENABLE_TABLE_META_CACHE", Some("true")),
            ("CACHE_DISK_PATH", Some("_cache_env")),
            ("CACHE_DISK_MAX_BYES", Some("512")),
            ("CACHE_TABLE_META_SNAPSHOT_COUNT", Some("256")),
            ("CACHE_TABLE_META_SEGMENT_BYTES", Some("10240")),
            ("META_ENDPOINTS", Some("0.0.0.0:9191")),
            ("CACHE_TABLE_BLOOM_INDEX_META_COUNT", Some("3000")),
            (
                "CACHE_TABLE_BLOOM_INDEX_FILTER_COUNT",
                Some(format!("{}", 1024 * 1024 * 1024).as_str()),
            ),
            (
                "CACHE_TABLE_BLOOM_INDEX_FILTER_SIZE",
                Some(format!("{}", 2u64 * 1024 * 1024 * 1024).as_str()),
            ),
            ("STORAGE_TYPE", Some("s3")),
            ("STORAGE_NUM_CPUS", Some("16")),
            ("STORAGE_FS_DATA_PATH", Some("/tmp/test")),
            ("STORAGE_S3_REGION", Some("us.region")),
            ("STORAGE_S3_ENDPOINT_URL", Some("http://127.0.0.1:10024")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("us.key.id")),
            ("STORAGE_S3_SECRET_ACCESS_KEY", Some("us.key")),
            ("STORAGE_S3_BUCKET", Some("us.bucket")),
            (
                "STORAGE_GCS_ENDPOINT_URL",
                Some("http://gcs.storage.cname_map.local"),
            ),
            ("STORAGE_GCS_BUCKET", Some("gcs.bucket")),
            ("STORAGE_GCS_ROOT", Some("/path/to/root")),
            ("STORAGE_GCS_CREDENTIAL", Some("gcs.credential")),
            ("STORAGE_OSS_BUCKET", Some("oss.bucket")),
            (
                "STORAGE_OSS_ENDPOINT_URL",
                Some("https://oss-cn-litang.example.com"),
            ),
            ("STORAGE_OSS_ROOT", Some("oss.root")),
            ("STORAGE_OSS_ACCESS_KEY_ID", Some("access_key_id")),
            ("STORAGE_OSS_ACCESS_KEY_SECRET", Some("access_key_secret")),
            ("STORAGE_WEBHDFS_DELEGATION", Some("delegation")),
            ("STORAGE_WEBHDFS_ENDPOINT_URL", Some("endpoint_url")),
            ("STORAGE_WEBHDFS_ROOT", Some("/path/to/root")),
            ("QUERY_TABLE_ENGINE_MEMORY_ENABLED", Some("true")),
            ("CONFIG_FILE", None),
        ],
        || {
            let configured = InnerConfig::load_for_test()
                .expect("must success")
                .into_config();

            assert_eq!("DEBUG", configured.log.level);

            assert_eq!("tenant-1", configured.query.tenant_id);
            assert_eq!("cluster-1", configured.query.cluster_id);
            assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
            assert_eq!(3306, configured.query.mysql_handler_port);
            assert_eq!(255, configured.query.max_active_sessions);
            assert_eq!("1.2.3.4", configured.query.clickhouse_http_handler_host);
            assert_eq!(8124, configured.query.clickhouse_http_handler_port);
            assert_eq!("1.2.3.4", configured.query.http_handler_host);
            assert_eq!(8001, configured.query.http_handler_port);

            assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
            assert_eq!("1.2.3.4:8081", configured.query.admin_api_address);
            assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

            assert_eq!(1, configured.meta.endpoints.len());
            assert_eq!("0.0.0.0:9191", configured.meta.endpoints[0]);

            assert_eq!("s3", configured.storage.typ);
            assert_eq!(16, configured.storage.storage_num_cpus);

            // config of fs should not be loaded, take default value.
            assert_eq!("_data", configured.storage.fs.data_path);

            // Storage type is fs, gcs related value should be default.
            assert_eq!(
                "https://storage.googleapis.com",
                configured.storage.gcs.gcs_endpoint_url
            );
            assert_eq!("", configured.storage.gcs.gcs_bucket);
            assert_eq!("", configured.storage.gcs.gcs_root);
            assert_eq!("", configured.storage.gcs.credential);

            assert_eq!("us.region", configured.storage.s3.region);
            assert_eq!("http://127.0.0.1:10024", configured.storage.s3.endpoint_url);
            assert_eq!("us.key.id", configured.storage.s3.access_key_id);
            assert_eq!("us.key", configured.storage.s3.secret_access_key);
            assert_eq!("us.bucket", configured.storage.s3.bucket);

            assert!(configured.cache.enable_table_meta_cache);
            assert!(configured.cache.enable_table_index_bloom);
            assert_eq!(10240, configured.cache.table_meta_segment_bytes);
            assert_eq!(256, configured.cache.table_meta_snapshot_count);
            assert_eq!(3000, configured.cache.table_bloom_index_meta_count);
            assert_eq!(
                1024 * 1024 * 1024,
                configured.cache.table_bloom_index_filter_count
            );
            assert_eq!(
                2 * 1024 * 1024 * 1024,
                configured.cache.table_bloom_index_filter_size
            );
            assert_eq!(HashMap::new(), configured.catalogs);
        },
    );

    Ok(())
}

// From env, defaulting.
#[test]
fn test_env_config_fs() -> anyhow::Result<()> {
    temp_env::with_vars(
        vec![
            ("LOG_LEVEL", Some("DEBUG")),
            ("QUERY_TENANT_ID", Some("tenant-1")),
            ("QUERY_CLUSTER_ID", Some("cluster-1")),
            ("QUERY_MYSQL_HANDLER_HOST", Some("127.0.0.1")),
            ("QUERY_MYSQL_HANDLER_PORT", Some("3306")),
            ("QUERY_MAX_ACTIVE_SESSIONS", Some("255")),
            ("QUERY_CLICKHOUSE_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HANDLER_PORT", Some("9000")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_PORT", Some("8124")),
            ("QUERY_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_HTTP_HANDLER_PORT", Some("8001")),
            ("QUERY_FLIGHT_API_ADDRESS", Some("1.2.3.4:9091")),
            ("QUERY_ADMIN_API_ADDRESS", Some("1.2.3.4:8081")),
            ("QUERY_METRIC_API_ADDRESS", Some("1.2.3.4:7071")),
            ("CACHE_ENABLE_TABLE_META_CACHE", Some("true")),
            ("CACHE_DISK_PATH", Some("_cache_env")),
            ("CACHE_DISK_MAX_BYTES", Some("512")),
            ("CACHE_TABLE_META_SNAPSHOT_COUNT", Some("256")),
            ("CACHE_TABLE_META_SEGMENT_BYTES", Some("1024000")),
            ("META_ENDPOINTS", Some("0.0.0.0:9191")),
            ("CACHE_TABLE_BLOOM_INDEX_META_COUNT", Some("3000")),
            (
                "CACHE_TABLE_BLOOM_INDEX_FILTER_COUNT",
                Some(format!("{}", 1024 * 1024 * 1024).as_str()),
            ),
            (
                "CACHE_TABLE_BLOOM_INDEX_FILTER_SIZE",
                Some(format!("{}", 2u64 * 1024 * 1024 * 1024).as_str()),
            ),
            ("STORAGE_TYPE", Some("fs")),
            ("STORAGE_NUM_CPUS", Some("16")),
            ("STORAGE_FS_DATA_PATH", Some("/tmp/test")),
            ("STORAGE_S3_REGION", Some("us.region")),
            ("STORAGE_S3_ENDPOINT_URL", Some("http://127.0.0.1:10024")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("us.key.id")),
            ("STORAGE_S3_SECRET_ACCESS_KEY", Some("us.key")),
            ("STORAGE_S3_BUCKET", Some("us.bucket")),
            (
                "STORAGE_GCS_ENDPOINT_URL",
                Some("http://gcs.storage.cname_map.local"),
            ),
            ("STORAGE_GCS_BUCKET", Some("gcs.bucket")),
            ("STORAGE_GCS_ROOT", Some("/path/to/root")),
            ("STORAGE_GCS_CREDENTIAL", Some("gcs.credential")),
            ("STORAGE_OSS_BUCKET", Some("oss.bucket")),
            (
                "STORAGE_OSS_ENDPOINT_URL",
                Some("https://oss-cn-litang.example.com"),
            ),
            ("STORAGE_OSS_ROOT", Some("oss.root")),
            ("STORAGE_OSS_ACCESS_KEY_ID", Some("access_key_id")),
            ("STORAGE_OSS_ACCESS_KEY_SECRET", Some("access_key_secret")),
            ("QUERY_TABLE_ENGINE_MEMORY_ENABLED", Some("true")),
            ("CONFIG_FILE", None),
        ],
        || {
            let configured = InnerConfig::load_for_test()
                .expect("must success")
                .into_config();

            assert_eq!("DEBUG", configured.log.level);

            assert_eq!("tenant-1", configured.query.tenant_id);
            assert_eq!("cluster-1", configured.query.cluster_id);
            assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
            assert_eq!(3306, configured.query.mysql_handler_port);
            assert_eq!(255, configured.query.max_active_sessions);
            assert_eq!("1.2.3.4", configured.query.clickhouse_http_handler_host);
            assert_eq!(8124, configured.query.clickhouse_http_handler_port);
            assert_eq!("1.2.3.4", configured.query.http_handler_host);
            assert_eq!(8001, configured.query.http_handler_port);

            assert_eq!(1, configured.meta.endpoints.len());
            assert_eq!("0.0.0.0:9191", configured.meta.endpoints[0]);

            assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
            assert_eq!("1.2.3.4:8081", configured.query.admin_api_address);
            assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

            assert_eq!("fs", configured.storage.typ);
            assert_eq!(16, configured.storage.storage_num_cpus);

            assert_eq!("/tmp/test", configured.storage.fs.data_path);

            // Storage type is fs, s3 related value should be default.
            assert_eq!("", configured.storage.s3.region);
            assert_eq!(
                "https://s3.amazonaws.com",
                configured.storage.s3.endpoint_url
            );

            // Storage type is fs, gcs related value should be default.
            assert_eq!(
                "https://storage.googleapis.com",
                configured.storage.gcs.gcs_endpoint_url
            );
            assert_eq!("", configured.storage.gcs.gcs_bucket);
            assert_eq!("", configured.storage.gcs.gcs_root);
            assert_eq!("", configured.storage.gcs.credential);

            assert!(configured.cache.enable_table_index_bloom);
            assert!(configured.cache.enable_table_meta_cache);
            assert_eq!("_cache_env", configured.cache.disk_cache_config.path);
            assert_eq!(512, configured.cache.disk_cache_config.max_bytes);
            assert_eq!(1024000, configured.cache.table_meta_segment_bytes);
            assert_eq!(256, configured.cache.table_meta_snapshot_count);
            assert_eq!(3000, configured.cache.table_bloom_index_meta_count);
            assert_eq!(
                1024 * 1024 * 1024,
                configured.cache.table_bloom_index_filter_count
            );
            assert_eq!(
                2 * 1024 * 1024 * 1024,
                configured.cache.table_bloom_index_filter_size
            );
        },
    );

    Ok(())
}

#[test]
fn test_env_config_gcs() -> anyhow::Result<()> {
    temp_env::with_vars(
        vec![
            ("LOG_LEVEL", Some("DEBUG")),
            ("QUERY_TENANT_ID", Some("tenant-1")),
            ("QUERY_CLUSTER_ID", Some("cluster-1")),
            ("QUERY_MYSQL_HANDLER_HOST", Some("127.0.0.1")),
            ("QUERY_MYSQL_HANDLER_PORT", Some("3306")),
            ("QUERY_MAX_ACTIVE_SESSIONS", Some("255")),
            ("QUERY_CLICKHOUSE_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HANDLER_PORT", Some("9000")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_PORT", Some("8124")),
            ("QUERY_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_HTTP_HANDLER_PORT", Some("8001")),
            ("QUERY_FLIGHT_API_ADDRESS", Some("1.2.3.4:9091")),
            ("QUERY_ADMIN_API_ADDRESS", Some("1.2.3.4:8081")),
            ("QUERY_METRIC_API_ADDRESS", Some("1.2.3.4:7071")),
            ("CACHE_ENABLE_TABLE_META_CACHE", Some("true")),
            ("CACHE_DISK_PATH", Some("_cache_env")),
            ("CACHE_DISK_MAX_BYTES", Some("512")),
            ("CACHE_TABLE_META_SNAPSHOT_COUNT", Some("256")),
            ("CACHE_TABLE_META_SEGMENT_BYTES", Some("10240")),
            ("META_ENDPOINTS", Some("0.0.0.0:9191")),
            ("CACHE_TABLE_BLOOM_INDEX_META_COUNT", Some("3000")),
            (
                "CACHE_TABLE_BLOOM_INDEX_FILTER_COUNT",
                Some(format!("{}", 1024 * 1024 * 1024).as_str()),
            ),
            (
                "CACHE_TABLE_BLOOM_INDEX_FILTER_SIZE",
                Some(format!("{}", 3u64 * 1024 * 1024 * 1024).as_str()),
            ),
            ("STORAGE_TYPE", Some("gcs")),
            ("STORAGE_NUM_CPUS", Some("16")),
            ("STORAGE_FS_DATA_PATH", Some("/tmp/test")),
            ("STORAGE_S3_REGION", Some("us.region")),
            ("STORAGE_S3_ENDPOINT_URL", Some("http://127.0.0.1:10024")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("us.key.id")),
            ("STORAGE_S3_SECRET_ACCESS_KEY", Some("us.key")),
            ("STORAGE_S3_BUCKET", Some("us.bucket")),
            (
                "STORAGE_GCS_ENDPOINT_URL",
                Some("http://gcs.storage.cname_map.local"),
            ),
            ("STORAGE_GCS_BUCKET", Some("gcs.bucket")),
            ("STORAGE_GCS_ROOT", Some("/path/to/root")),
            ("STORAGE_GCS_CREDENTIAL", Some("gcs.credential")),
            ("STORAGE_OSS_BUCKET", Some("oss.bucket")),
            (
                "STORAGE_OSS_ENDPOINT_URL",
                Some("https://oss-cn-litang.example.com"),
            ),
            ("STORAGE_OSS_ROOT", Some("oss.root")),
            ("STORAGE_OSS_ACCESS_KEY_ID", Some("access_key_id")),
            ("STORAGE_OSS_ACCESS_KEY_SECRET", Some("access_key_secret")),
            ("QUERY_TABLE_ENGINE_MEMORY_ENABLED", Some("true")),
            ("CONFIG_FILE", None),
        ],
        || {
            let configured = InnerConfig::load_for_test()
                .expect("must success")
                .into_config();

            assert_eq!("DEBUG", configured.log.level);

            assert_eq!("tenant-1", configured.query.tenant_id);
            assert_eq!("cluster-1", configured.query.cluster_id);
            assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
            assert_eq!(3306, configured.query.mysql_handler_port);
            assert_eq!(255, configured.query.max_active_sessions);
            assert_eq!("1.2.3.4", configured.query.clickhouse_http_handler_host);
            assert_eq!(8124, configured.query.clickhouse_http_handler_port);
            assert_eq!("1.2.3.4", configured.query.http_handler_host);
            assert_eq!(8001, configured.query.http_handler_port);

            assert_eq!(1, configured.meta.endpoints.len());
            assert_eq!("0.0.0.0:9191", configured.meta.endpoints[0]);

            assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
            assert_eq!("1.2.3.4:8081", configured.query.admin_api_address);
            assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

            assert_eq!("gcs", configured.storage.typ);
            assert_eq!(16, configured.storage.storage_num_cpus);

            assert_eq!(
                "http://gcs.storage.cname_map.local",
                configured.storage.gcs.gcs_endpoint_url
            );
            assert_eq!("gcs.bucket", configured.storage.gcs.gcs_bucket);
            assert_eq!("/path/to/root", configured.storage.gcs.gcs_root);
            assert_eq!("gcs.credential", configured.storage.gcs.credential);

            // Storage type is gcs, fs related value should stay default.
            assert_eq!("_data", configured.storage.fs.data_path);

            // Storage type is gcs, s3 related value should be default.
            assert_eq!("", configured.storage.s3.region);
            assert_eq!(
                "https://s3.amazonaws.com",
                configured.storage.s3.endpoint_url
            );

            // Storage type is gcs, oss related value should be default.
            assert_eq!("", configured.storage.oss.oss_endpoint_url);
            assert_eq!("", configured.storage.oss.oss_bucket);
            assert_eq!("", configured.storage.oss.oss_root);
            assert_eq!("", configured.storage.oss.oss_access_key_id);
            assert_eq!("", configured.storage.oss.oss_access_key_secret);

            assert!(configured.cache.enable_table_meta_cache);
            assert!(configured.cache.enable_table_index_bloom);
            assert_eq!("_cache_env", configured.cache.disk_cache_config.path);
            assert_eq!(512, configured.cache.disk_cache_config.max_bytes);
            assert_eq!(10240, configured.cache.table_meta_segment_bytes);
            assert_eq!(256, configured.cache.table_meta_snapshot_count);
            assert_eq!(3000, configured.cache.table_bloom_index_meta_count);
            assert_eq!(
                1024 * 1024 * 1024,
                configured.cache.table_bloom_index_filter_count
            );
            assert_eq!(
                3 * 1024 * 1024 * 1024,
                configured.cache.table_bloom_index_filter_size
            );
        },
    );

    Ok(())
}

#[test]
fn test_env_config_oss() -> anyhow::Result<()> {
    temp_env::with_vars(
        vec![
            ("LOG_LEVEL", Some("DEBUG")),
            ("QUERY_TENANT_ID", Some("tenant-1")),
            ("QUERY_CLUSTER_ID", Some("cluster-1")),
            ("QUERY_MYSQL_HANDLER_HOST", Some("127.0.0.1")),
            ("QUERY_MYSQL_HANDLER_PORT", Some("3306")),
            ("QUERY_MAX_ACTIVE_SESSIONS", Some("255")),
            ("QUERY_CLICKHOUSE_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HANDLER_PORT", Some("9000")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_PORT", Some("8124")),
            ("QUERY_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_HTTP_HANDLER_PORT", Some("8001")),
            ("QUERY_FLIGHT_API_ADDRESS", Some("1.2.3.4:9091")),
            ("QUERY_ADMIN_API_ADDRESS", Some("1.2.3.4:8081")),
            ("QUERY_METRIC_API_ADDRESS", Some("1.2.3.4:7071")),
            ("CACHE_ENABLE_TABLE_META_CACHE", Some("true")),
            ("CACHE_DATA_CACHE_STORAGE", Some("disk")),
            ("TABLE_CACHE_BLOOM_INDEX_FILTER_COUNT", Some("1")),
            ("CACHE_DISK_PATH", Some("_cache_env")),
            ("CACHE_DISK_MAX_BYTES", Some("512")),
            ("CACHE_TABLE_META_SNAPSHOT_COUNT", Some("256")),
            ("CACHE_TABLE_META_SEGMENT_BYTES", Some("10240")),
            ("META_ENDPOINTS", Some("0.0.0.0:9191")),
            ("CACHE_TABLE_BLOOM_INDEX_META_COUNT", Some("3000")),
            (
                "CACHE_TABLE_BLOOM_INDEX_FILTER_COUNT",
                Some(format!("{}", 1024 * 1024 * 1024).as_str()),
            ),
            (
                "CACHE_TABLE_BLOOM_INDEX_FILTER_SIZE",
                Some(format!("{}", 4u64 * 1024 * 1024 * 1024).as_str()),
            ),
            ("STORAGE_TYPE", Some("oss")),
            ("STORAGE_NUM_CPUS", Some("16")),
            ("STORAGE_FS_DATA_PATH", Some("/tmp/test")),
            ("STORAGE_S3_REGION", Some("us.region")),
            ("STORAGE_S3_ENDPOINT_URL", Some("http://127.0.0.1:10024")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("us.key.id")),
            ("STORAGE_S3_SECRET_ACCESS_KEY", Some("us.key")),
            ("STORAGE_S3_BUCKET", Some("us.bucket")),
            (
                "STORAGE_GCS_ENDPOINT_URL",
                Some("http://gcs.storage.cname_map.local"),
            ),
            ("STORAGE_GCS_BUCKET", Some("gcs.bucket")),
            ("STORAGE_GCS_ROOT", Some("/path/to/root")),
            ("STORAGE_GCS_CREDENTIAL", Some("gcs.credential")),
            ("STORAGE_OSS_BUCKET", Some("oss.bucket")),
            (
                "STORAGE_OSS_ENDPOINT_URL",
                Some("https://oss-cn-litang.example.com"),
            ),
            ("STORAGE_OSS_ROOT", Some("oss.root")),
            ("STORAGE_OSS_ACCESS_KEY_ID", Some("access_key_id")),
            ("STORAGE_OSS_ACCESS_KEY_SECRET", Some("access_key_secret")),
            ("STORAGE_WEBHDFS_DELEGATION", Some("delegation")),
            ("STORAGE_WEBHDFS_ENDPOINT_URL", Some("endpoint_url")),
            ("STORAGE_WEBHDFS_ROOT", Some("/path/to/root")),
            ("QUERY_TABLE_ENGINE_MEMORY_ENABLED", Some("true")),
            ("CONFIG_FILE", None),
        ],
        || {
            let configured = InnerConfig::load_for_test()
                .expect("must success")
                .into_config();

            assert_eq!("DEBUG", configured.log.level);

            assert_eq!("tenant-1", configured.query.tenant_id);
            assert_eq!("cluster-1", configured.query.cluster_id);
            assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
            assert_eq!(3306, configured.query.mysql_handler_port);
            assert_eq!(255, configured.query.max_active_sessions);
            assert_eq!("1.2.3.4", configured.query.clickhouse_http_handler_host);
            assert_eq!(8124, configured.query.clickhouse_http_handler_port);
            assert_eq!("1.2.3.4", configured.query.http_handler_host);
            assert_eq!(8001, configured.query.http_handler_port);

            assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
            assert_eq!("1.2.3.4:8081", configured.query.admin_api_address);
            assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

            assert_eq!(1, configured.meta.endpoints.len());
            assert_eq!("0.0.0.0:9191", configured.meta.endpoints[0]);

            assert_eq!("oss", configured.storage.typ);
            assert_eq!(16, configured.storage.storage_num_cpus);

            // Storage type is oss, s3 related value should be default.
            assert_eq!("", configured.storage.s3.region);
            assert_eq!(
                "https://s3.amazonaws.com",
                configured.storage.s3.endpoint_url
            );

            // config of fs should not be loaded, take default value.
            assert_eq!("_data", configured.storage.fs.data_path);

            // Storage type is oss, gcs related value should be default.
            assert_eq!(
                "https://storage.googleapis.com",
                configured.storage.gcs.gcs_endpoint_url
            );

            assert_eq!(
                "https://oss-cn-litang.example.com",
                configured.storage.oss.oss_endpoint_url
            );
            assert_eq!("oss.bucket", configured.storage.oss.oss_bucket);
            assert_eq!("oss.root", configured.storage.oss.oss_root);
            assert_eq!("access_key_id", configured.storage.oss.oss_access_key_id);
            assert_eq!(
                "access_key_secret",
                configured.storage.oss.oss_access_key_secret
            );

            assert_eq!("", configured.storage.gcs.gcs_bucket);
            assert_eq!("", configured.storage.gcs.gcs_root);
            assert_eq!("", configured.storage.gcs.credential);

            assert!(configured.cache.enable_table_meta_cache);
            assert_eq!("_cache_env", configured.cache.disk_cache_config.path);
            assert_eq!(512, configured.cache.disk_cache_config.max_bytes);
            assert_eq!(10240, configured.cache.table_meta_segment_bytes);
            assert_eq!(256, configured.cache.table_meta_snapshot_count);
            assert_eq!(3000, configured.cache.table_bloom_index_meta_count);
            assert_eq!(
                1024 * 1024 * 1024,
                configured.cache.table_bloom_index_filter_count
            );
            assert_eq!(
                4 * 1024 * 1024 * 1024,
                configured.cache.table_bloom_index_filter_size
            );
        },
    );
    Ok(())
}

#[test]
fn test_env_config_webhdfs() -> anyhow::Result<()> {
    temp_env::with_vars(
        vec![
            ("LOG_LEVEL", Some("DEBUG")),
            ("QUERY_TENANT_ID", Some("tenant-1")),
            ("QUERY_CLUSTER_ID", Some("cluster-1")),
            ("QUERY_MYSQL_HANDLER_HOST", Some("127.0.0.1")),
            ("QUERY_MYSQL_HANDLER_PORT", Some("3306")),
            ("QUERY_MAX_ACTIVE_SESSIONS", Some("255")),
            ("QUERY_CLICKHOUSE_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HANDLER_PORT", Some("9000")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_PORT", Some("8124")),
            ("QUERY_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_HTTP_HANDLER_PORT", Some("8001")),
            ("QUERY_FLIGHT_API_ADDRESS", Some("1.2.3.4:9091")),
            ("QUERY_ADMIN_API_ADDRESS", Some("1.2.3.4:8081")),
            ("QUERY_METRIC_API_ADDRESS", Some("1.2.3.4:7071")),
            ("CACHE_ENABLE_TABLE_META_CACHE", Some("true")),
            ("CACHE_DATA_CACHE_STORAGE", Some("disk")),
            ("TABLE_CACHE_BLOOM_INDEX_FILTER_COUNT", Some("1")),
            ("CACHE_DISK_PATH", Some("_cache_env")),
            ("CACHE_DISK_MAX_BYTES", Some("512")),
            ("CACHE_TABLE_META_SNAPSHOT_COUNT", Some("256")),
            ("CACHE_TABLE_META_SEGMENT_BYTES", Some("10240")),
            ("META_ENDPOINTS", Some("0.0.0.0:9191")),
            ("CACHE_TABLE_BLOOM_INDEX_META_COUNT", Some("3000")),
            (
                "CACHE_TABLE_BLOOM_INDEX_FILTER_COUNT",
                Some(format!("{}", 1024 * 1024 * 1024).as_str()),
            ),
            ("STORAGE_TYPE", Some("webhdfs")),
            ("STORAGE_NUM_CPUS", Some("16")),
            ("STORAGE_FS_DATA_PATH", Some("/tmp/test")),
            ("STORAGE_S3_REGION", Some("us.region")),
            ("STORAGE_S3_ENDPOINT_URL", Some("http://127.0.0.1:10024")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("us.key.id")),
            ("STORAGE_S3_SECRET_ACCESS_KEY", Some("us.key")),
            ("STORAGE_S3_BUCKET", Some("us.bucket")),
            (
                "STORAGE_GCS_ENDPOINT_URL",
                Some("http://gcs.storage.cname_map.local"),
            ),
            ("STORAGE_GCS_BUCKET", Some("gcs.bucket")),
            ("STORAGE_GCS_ROOT", Some("/path/to/root")),
            ("STORAGE_GCS_CREDENTIAL", Some("gcs.credential")),
            ("STORAGE_OSS_BUCKET", Some("oss.bucket")),
            (
                "STORAGE_OSS_ENDPOINT_URL",
                Some("https://oss-cn-litang.example.com"),
            ),
            ("STORAGE_OSS_ROOT", Some("oss.root")),
            ("STORAGE_OSS_ACCESS_KEY_ID", Some("access_key_id")),
            ("STORAGE_OSS_ACCESS_KEY_SECRET", Some("access_key_secret")),
            ("STORAGE_WEBHDFS_DELEGATION", Some("delegation")),
            ("STORAGE_WEBHDFS_ENDPOINT_URL", Some("endpoint_url")),
            ("STORAGE_WEBHDFS_ROOT", Some("/path/to/root")),
            ("QUERY_TABLE_ENGINE_MEMORY_ENABLED", Some("true")),
            ("CONFIG_FILE", None),
        ],
        || {
            let configured = InnerConfig::load_for_test()
                .expect("must success")
                .into_config();

            assert_eq!("DEBUG", configured.log.level);

            assert_eq!("tenant-1", configured.query.tenant_id);
            assert_eq!("cluster-1", configured.query.cluster_id);
            assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
            assert_eq!(3306, configured.query.mysql_handler_port);
            assert_eq!(255, configured.query.max_active_sessions);
            assert_eq!("1.2.3.4", configured.query.clickhouse_http_handler_host);
            assert_eq!(8124, configured.query.clickhouse_http_handler_port);
            assert_eq!("1.2.3.4", configured.query.http_handler_host);
            assert_eq!(8001, configured.query.http_handler_port);

            assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
            assert_eq!("1.2.3.4:8081", configured.query.admin_api_address);
            assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

            assert_eq!(1, configured.meta.endpoints.len());
            assert_eq!("0.0.0.0:9191", configured.meta.endpoints[0]);

            assert_eq!("webhdfs", configured.storage.typ);
            assert_eq!(16, configured.storage.storage_num_cpus);

            // Storage type is webhdfs, s3 related value should be default.
            assert_eq!("", configured.storage.s3.region);
            assert_eq!(
                "https://s3.amazonaws.com",
                configured.storage.s3.endpoint_url
            );

            // config of fs should not be loaded, take default value.
            assert_eq!("_data", configured.storage.fs.data_path);

            // Storage type is webhdfs, gcs related value should be default.
            assert_eq!(
                "https://storage.googleapis.com",
                configured.storage.gcs.gcs_endpoint_url
            );

            // Storage type is webhdfs, should take default value.
            assert_eq!("", configured.storage.oss.oss_endpoint_url);
            assert_eq!("", configured.storage.oss.oss_bucket);
            assert_eq!("", configured.storage.oss.oss_root);
            assert_eq!("", configured.storage.oss.oss_access_key_id);
            assert_eq!("", configured.storage.oss.oss_access_key_secret);

            assert_eq!("", configured.storage.gcs.gcs_bucket);
            assert_eq!("", configured.storage.gcs.gcs_root);
            assert_eq!("", configured.storage.gcs.credential);

            // assert webhdfs values
            assert_eq!("delegation", configured.storage.webhdfs.webhdfs_delegation);
            assert_eq!(
                "endpoint_url",
                configured.storage.webhdfs.webhdfs_endpoint_url
            );
            assert_eq!("/path/to/root", configured.storage.webhdfs.webhdfs_root);

            assert!(configured.cache.enable_table_meta_cache);
            assert_eq!("_cache_env", configured.cache.disk_cache_config.path);
            assert_eq!(512, configured.cache.disk_cache_config.max_bytes);
            assert_eq!(10240, configured.cache.table_meta_segment_bytes);
            assert_eq!(256, configured.cache.table_meta_snapshot_count);
            assert_eq!(3000, configured.cache.table_bloom_index_meta_count);
            assert_eq!(
                1024 * 1024 * 1024,
                configured.cache.table_bloom_index_filter_count
            );
        },
    );
    Ok(())
}

/// Test whether override works as expected.
#[test]
fn test_override_config() -> anyhow::Result<()> {
    let file_path = temp_dir().join("databend_test_config.toml");

    let mut f = fs::File::create(&file_path)?;
    f.write_all(
        r#"config_file = ""

[query]
tenant_id = "tenant_id_from_file"
cluster_id = ""
num_cpus = 0
mysql_handler_host = "127.0.0.1"
mysql_handler_port = 3307
max_active_sessions = 256
max_server_memory_usage = 0
clickhouse_handler_host = "127.0.0.1"
clickhouse_handler_port = 9000
clickhouse_http_handler_host = "127.0.0.1"
clickhouse_http_handler_port = 8124
http_handler_host = "127.0.0.1"
http_handler_port = 8000
http_handler_result_timeout_secs = 60
flight_api_address = "127.0.0.1:9090"
admin_api_address = "127.0.0.1:8080"
metric_api_address = "127.0.0.1:7070"
http_handler_tls_server_cert = ""
http_handler_tls_server_key = ""
http_handler_tls_server_root_ca_cert = ""
api_tls_server_cert = ""
api_tls_server_key = ""
api_tls_server_root_ca_cert = ""
rpc_tls_server_cert = ""
rpc_tls_server_key = ""
rpc_tls_query_server_root_ca_cert = ""
rpc_tls_query_service_domain_name = "localhost"
table_engine_memory_enabled = true
shutdown_wait_timeout_ms = 5000
max_query_log_size = 10000
management_mode = false
jwt_key_file = ""
users = []
share_endpoint_address = ""

[log]
level = "INFO"
dir = "./.databend/logs"

[log.query]
on = false

[meta]
endpoints = ["0.0.0.0:9191"]
username = "username_from_file"
password = "password_from_file"
client_timeout_in_second = 4
rpc_tls_meta_server_root_ca_cert = ""
rpc_tls_meta_service_domain_name = "localhost"

[storage]
type = "s3"
num_cpus = 0

[storage.fs]
data_path = "./.databend/data"

[storage.s3]
region = ""
endpoint_url = "https://s3.amazonaws.com"
access_key_id = "access_key_id_from_file"
secret_access_key = ""
bucket = ""
root = ""
master_key = ""

[storage.azblob]
account_name = ""
account_key = ""
container = ""
endpoint_url = ""
root = ""

[storage.hdfs]
name_node = ""
root = ""

[storage.obs]
endpoint_url = ""
access_key_id = ""
secret_access_key = ""
bucket = ""
root = ""

[storage.oss]
endpoint_url = ""
access_key_id = ""
access_key_secret = ""
bucket = ""
root = ""

[storage.webhdfs]
endpoint_url = ""
delegation = ""
root = ""

[catalog]
address = "127.0.0.1:9083"
protocol = "binary"

[catalogs.my_hive]
type = "hive"
address = "127.0.0.1:9083"
protocol = "binary"

[cache]

enable_table_meta_cache = false
table_meta_snapshot_count = 256
table_meta_segment_bytes = 10240
table_bloom_index_meta_count = 3000
table_bloom_index_filter_count = 1048576

data_cache_storage = "disk"

[cache.disk]
path = "_cache"
"#
        .as_bytes(),
    )?;

    // Make sure all data flushed.
    f.flush()?;

    temp_env::with_vars(
        vec![
            ("CONFIG_FILE", Some(file_path.to_string_lossy().as_ref())),
            ("QUERY_TENANT_ID", Some("tenant_id_from_env")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("access_key_id_from_env")),
            ("STORAGE_TYPE", None),
        ],
        || {
            let cfg = InnerConfig::load_for_test()
                .expect("config load success")
                .into_config();

            assert!(!cfg.log.query.log_query_on);

            assert_eq!("tenant_id_from_env", cfg.query.tenant_id);
            assert_eq!("access_key_id_from_env", cfg.storage.s3.access_key_id);
            assert_eq!("s3", cfg.storage.typ);

            let cache_config = &cfg.cache;
            assert_eq!(
                cache_config.data_cache_storage,
                CacheStorageTypeConfig::Disk
            );
            assert_eq!(cache_config.disk_cache_config.path, "_cache");

            // NOTE:
            //
            // after the config conversion procedure:
            // Outer -> Inner -> Outer
            //
            // config in `catalog` field will be moved to `catalogs` field
            assert!(cfg.catalog.address.is_empty());
            assert!(cfg.catalog.protocol.is_empty());
            // config in `catalog` field, with name of "hive"
            assert!(cfg.catalogs.contains_key("hive"), "catalogs is none!");
            // config in `catalogs` field, with name of "my_hive"
            assert!(cfg.catalogs.contains_key("my_hive"), "catalogs is none!");

            let inner: CatalogConfig = cfg.catalogs["my_hive"].clone();
            assert_eq!("hive", inner.ty);
            assert_eq!(
                "127.0.0.1:9083", inner.hive.metastore_address,
                "address incorrect"
            );
            assert_eq!("binary", inner.hive.protocol, "protocol incorrect");
        },
    );

    // remove temp file
    fs::remove_file(file_path)?;

    Ok(())
}

/// Test old hive catalog
#[test]
fn test_override_config_old_hive_catalog() -> anyhow::Result<()> {
    let file_path = temp_dir().join("databend_test_override_config_old_hive_catalog.toml");

    let mut f = fs::File::create(&file_path)?;
    f.write_all(
        r#"
[catalog]
address = "1.1.1.1:10000"
protocol = "binary"
"#
        .as_bytes(),
    )?;

    // Make sure all data flushed.
    f.flush()?;

    temp_env::with_vars(
        vec![("CONFIG_FILE", Some(file_path.to_string_lossy().as_ref()))],
        || {
            let cfg = InnerConfig::load_for_test().expect("config load success");

            assert_eq!(cfg.catalogs["hive"], CatalogConfig {
                ty: "hive".to_string(),
                hive: CatalogHiveConfig {
                    metastore_address: "1.1.1.1:10000".to_string(),
                    meta_store_address: None,
                    protocol: ThriftProtocol::Binary.to_string(),
                },
            });
        },
    );

    // remove temp file
    fs::remove_file(file_path)?;

    Ok(())
}

#[test]
fn test_spill_config() -> anyhow::Result<()> {
    let file_path = temp_dir().join("databend_test_spill_config.toml");

    let mut f = fs::File::create(&file_path)?;
    f.write_all(
        r#"
[spill]
spill_local_disk_path = "/data/spill"
"#
        .as_bytes(),
    )?;

    // Make sure all data flushed.
    f.flush()?;

    temp_env::with_vars(
        vec![("CONFIG_FILE", Some(file_path.to_string_lossy().as_ref()))],
        || {
            let cfg = InnerConfig::load_for_test().expect("config load failed");

            assert_eq!(cfg.spill.local_path(), Some("/data/spill".into()));
            assert_eq!(cfg.spill.reserved_disk_ratio, 0.1);
        },
    );

    // remove temp file
    fs::remove_file(file_path)?;

    Ok(())
}

/// Test new hive catalog
#[test]
fn test_override_config_new_hive_catalog() -> anyhow::Result<()> {
    let file_path = temp_dir().join("databend_test_override_config_new_hive_catalog.toml");

    let mut f = fs::File::create(&file_path)?;
    f.write_all(
        r#"
[catalogs.my_hive]
type = "hive"
address = "1.1.1.1:12000"
protocol = "binary"
"#
        .as_bytes(),
    )?;

    // Make sure all data flushed.
    f.flush()?;

    temp_env::with_vars(
        vec![("CONFIG_FILE", Some(file_path.to_string_lossy().as_ref()))],
        || {
            let cfg = InnerConfig::load_for_test().expect("config load success");

            assert_eq!(cfg.catalogs["my_hive"], CatalogConfig {
                ty: "hive".to_string(),
                hive: CatalogHiveConfig {
                    metastore_address: "1.1.1.1:12000".to_string(),
                    meta_store_address: None,
                    protocol: ThriftProtocol::Binary.to_string(),
                },
            });
        },
    );

    // remove temp file
    fs::remove_file(file_path)?;

    Ok(())
}

#[test]
fn test_env_config_obsoleted() -> anyhow::Result<()> {
    let obsoleted = vec![
        ("QUERY_TABLE_DISK_CACHE_MB_SIZE", Some("1")),
        ("QUERY_TABLE_META_CACHE_ENABLED", Some("true")),
        ("QUERY_TABLE_CACHE_BLOCK_META_COUNT", Some("1")),
        ("QUERY_TABLE_MEMORY_CACHE_MB_SIZE", Some("1")),
        ("QUERY_TABLE_DISK_CACHE_ROOT", Some("1")),
        ("QUERY_TABLE_CACHE_SNAPSHOT_COUNT", Some("1")),
        ("QUERY_TABLE_CACHE_STATISTIC_COUNT", Some("1")),
        ("QUERY_TABLE_CACHE_SEGMENT_COUNT", Some("1")),
        ("QUERY_TABLE_CACHE_BLOOM_INDEX_META_COUNT", Some("1")),
        ("QUERY_TABLE_CACHE_BLOOM_INDEX_FILTER_COUNT", Some("1")),
        ("QUERY_TABLE_CACHE_BLOOM_INDEX_DATA_BYTES", Some("1")),
    ];

    for env_var in obsoleted {
        temp_env::with_vars(vec![env_var], || {
            let r = InnerConfig::load_for_test();
            assert!(r.is_err(), "expecting `Err`, but got `Ok`");
            assert_eq!(r.unwrap_err().code(), ErrorCode::INVALID_CONFIG)
        });
    }

    Ok(())
}

#[test]
fn test_env_cache_config_and_defaults() -> anyhow::Result<()> {
    // test if one of the cache config option is overridden by environment variable
    // default values of other cache config options are correct
    //
    // @see issue https://github.com/datafuselabs/databend/issues/10088
    temp_env::with_vars(
        vec![("CACHE_ENABLE_TABLE_META_CACHE", Some("true"))],
        || {
            let configured = InnerConfig::load_for_test()
                .expect("must success")
                .into_config();

            let default = CacheConfig::default();
            assert!(configured.cache.enable_table_meta_cache);
            assert_eq!(
                default.table_meta_segment_bytes,
                configured.cache.table_meta_segment_bytes
            );
            assert_eq!(
                default.table_meta_snapshot_count,
                configured.cache.table_meta_snapshot_count
            );
        },
    );
    Ok(())
}
