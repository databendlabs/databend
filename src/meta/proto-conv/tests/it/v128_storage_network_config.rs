// Copyright 2023 Datafuse Labs.
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

use databend_common_meta_app::storage::StorageCosConfig;
use databend_common_meta_app::storage::StorageGcsConfig;
use databend_common_meta_app::storage::StorageHdfsConfig;
use databend_common_meta_app::storage::StorageHuggingfaceConfig;
use databend_common_meta_app::storage::StorageNetworkParams;
use databend_common_meta_app::storage::StorageObsConfig;
use databend_common_meta_app::storage::StorageOssConfig;
use databend_common_meta_app::storage::StorageS3Config;
use fastrace::func_name;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v128_storage_network_config() -> anyhow::Result<()> {
    let storage_huggingface_config_v128 = vec![
        10, 28, 111, 112, 101, 110, 100, 97, 108, 47, 104, 117, 103, 103, 105, 110, 103, 102, 97,
        99, 101, 45, 116, 101, 115, 116, 100, 97, 116, 97, 18, 8, 100, 97, 116, 97, 115, 101, 116,
        115, 26, 4, 109, 97, 105, 110, 34, 1, 47, 50, 19, 8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 48, 6,
        160, 6, 128, 1, 168, 6, 24, 160, 6, 128, 1, 168, 6, 24,
    ];

    let want = || StorageHuggingfaceConfig {
        repo_id: "opendal/huggingface-testdata".to_string(),
        repo_type: "datasets".to_string(),
        revision: "main".to_string(),
        token: "".to_string(),
        root: "/".to_string(),
        network_config: Some(StorageNetworkParams {
            retry_timeout: 1,
            retry_io_timeout: 2,
            tcp_keepalive: 3,
            connect_timeout: 4,
            pool_max_idle_per_host: 5,
            max_concurrent_io_requests: 6,
        }),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        storage_huggingface_config_v128.as_slice(),
        128,
        want(),
    )?;

    let storage_gcs_config_v128 = vec![
        10, 30, 104, 116, 116, 112, 115, 58, 47, 47, 115, 116, 111, 114, 97, 103, 101, 46, 103,
        111, 111, 103, 108, 101, 97, 112, 105, 115, 46, 99, 111, 109, 18, 9, 109, 121, 95, 98, 117,
        99, 107, 101, 116, 26, 11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 34, 13, 109,
        121, 95, 99, 114, 101, 100, 101, 110, 116, 105, 97, 108, 42, 19, 8, 1, 16, 2, 24, 3, 32, 4,
        40, 5, 48, 6, 160, 6, 128, 1, 168, 6, 24, 160, 6, 128, 1, 168, 6, 24,
    ];

    let want = || StorageGcsConfig {
        endpoint_url: "https://storage.googleapis.com".to_string(),
        bucket: "my_bucket".to_string(),
        root: "/data/files".to_string(),
        credential: "my_credential".to_string(),
        network_config: Some(StorageNetworkParams {
            retry_timeout: 1,
            retry_io_timeout: 2,
            tcp_keepalive: 3,
            connect_timeout: 4,
            pool_max_idle_per_host: 5,
            max_concurrent_io_requests: 6,
        }),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        storage_gcs_config_v128.as_slice(),
        128,
        want(),
    )?;

    let storage_s_3_config_v128 = vec![
        18, 24, 104, 116, 116, 112, 115, 58, 47, 47, 115, 51, 46, 97, 109, 97, 122, 111, 110, 97,
        119, 115, 46, 99, 111, 109, 26, 9, 109, 121, 95, 107, 101, 121, 95, 105, 100, 34, 13, 109,
        121, 95, 115, 101, 99, 114, 101, 116, 95, 107, 101, 121, 42, 8, 109, 121, 98, 117, 99, 107,
        101, 116, 50, 11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 58, 13, 109, 121, 95,
        109, 97, 115, 116, 101, 114, 95, 107, 101, 121, 82, 17, 109, 121, 95, 115, 101, 99, 117,
        114, 105, 116, 121, 95, 116, 111, 107, 101, 110, 114, 19, 8, 1, 16, 2, 24, 3, 32, 4, 40, 5,
        48, 6, 160, 6, 128, 1, 168, 6, 24, 160, 6, 128, 1, 168, 6, 24,
    ];

    let want = || StorageS3Config {
        bucket: "mybucket".to_string(),
        root: "/data/files".to_string(),
        access_key_id: "my_key_id".to_string(),
        secret_access_key: "my_secret_key".to_string(),
        security_token: "my_security_token".to_string(),
        master_key: "my_master_key".to_string(),
        network_config: Some(StorageNetworkParams {
            retry_timeout: 1,
            retry_io_timeout: 2,
            tcp_keepalive: 3,
            connect_timeout: 4,
            pool_max_idle_per_host: 5,
            max_concurrent_io_requests: 6,
        }),
        ..Default::default()
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        storage_s_3_config_v128.as_slice(),
        128,
        want(),
    )?;

    let storage_oss_config_v128 = vec![
        10, 33, 104, 116, 116, 112, 115, 58, 47, 47, 111, 115, 115, 45, 99, 110, 45, 108, 105, 116,
        97, 110, 103, 46, 101, 120, 97, 109, 112, 108, 101, 46, 99, 111, 109, 18, 9, 109, 121, 95,
        98, 117, 99, 107, 101, 116, 26, 11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 34,
        13, 97, 99, 99, 101, 115, 115, 95, 107, 101, 121, 95, 105, 100, 42, 17, 97, 99, 99, 101,
        115, 115, 95, 107, 101, 121, 95, 115, 101, 99, 114, 101, 116, 82, 19, 8, 1, 16, 2, 24, 3,
        32, 4, 40, 5, 48, 6, 160, 6, 128, 1, 168, 6, 24, 160, 6, 128, 1, 168, 6, 24,
    ];

    let want = || StorageOssConfig {
        endpoint_url: "https://oss-cn-litang.example.com".to_string(),
        bucket: "my_bucket".to_string(),
        root: "/data/files".to_string(),
        server_side_encryption: "".to_string(),
        access_key_id: "access_key_id".to_string(),
        access_key_secret: "access_key_secret".to_string(),
        presign_endpoint_url: "".to_string(),
        server_side_encryption_key_id: "".to_string(),
        network_config: Some(StorageNetworkParams {
            retry_timeout: 1,
            retry_io_timeout: 2,
            tcp_keepalive: 3,
            connect_timeout: 4,
            pool_max_idle_per_host: 5,
            max_concurrent_io_requests: 6,
        }),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        storage_oss_config_v128.as_slice(),
        128,
        want(),
    )?;

    let storage_hdfs_config_v128 = vec![
        10, 20, 47, 112, 97, 116, 104, 47, 116, 111, 47, 115, 116, 97, 103, 101, 47, 102, 105, 108,
        101, 115, 18, 21, 104, 100, 102, 115, 58, 47, 47, 108, 111, 99, 97, 108, 104, 111, 115,
        116, 58, 56, 48, 50, 48, 26, 19, 8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 48, 6, 160, 6, 128, 1,
        168, 6, 24, 160, 6, 128, 1, 168, 6, 24,
    ];

    let want = || StorageHdfsConfig {
        root: "/path/to/stage/files".to_string(),
        name_node: "hdfs://localhost:8020".to_string(),
        network_config: Some(StorageNetworkParams {
            retry_timeout: 1,
            retry_io_timeout: 2,
            tcp_keepalive: 3,
            connect_timeout: 4,
            pool_max_idle_per_host: 5,
            max_concurrent_io_requests: 6,
        }),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        storage_hdfs_config_v128.as_slice(),
        128,
        want(),
    )?;

    let storage_obs_config_v128 = vec![
        10, 23, 104, 116, 116, 112, 115, 58, 47, 47, 111, 98, 115, 46, 101, 120, 97, 109, 112, 108,
        101, 46, 99, 111, 109, 18, 6, 98, 117, 99, 107, 101, 116, 26, 20, 47, 112, 97, 116, 104,
        47, 116, 111, 47, 115, 116, 97, 103, 101, 47, 102, 105, 108, 101, 115, 34, 13, 97, 99, 99,
        101, 115, 115, 95, 107, 101, 121, 95, 105, 100, 42, 17, 115, 101, 99, 114, 101, 116, 95,
        97, 99, 99, 101, 115, 115, 95, 107, 101, 121, 50, 19, 8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 48,
        6, 160, 6, 128, 1, 168, 6, 24, 160, 6, 128, 1, 168, 6, 24,
    ];

    let want = || StorageObsConfig {
        endpoint_url: "https://obs.example.com".to_string(),
        root: "/path/to/stage/files".to_string(),
        access_key_id: "access_key_id".to_string(),
        secret_access_key: "secret_access_key".to_string(),
        bucket: "bucket".to_string(),
        network_config: Some(StorageNetworkParams {
            retry_timeout: 1,
            retry_io_timeout: 2,
            tcp_keepalive: 3,
            connect_timeout: 4,
            pool_max_idle_per_host: 5,
            max_concurrent_io_requests: 6,
        }),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        storage_obs_config_v128.as_slice(),
        128,
        want(),
    )?;

    let storage_cos_config_v128 = vec![
        10, 23, 104, 116, 116, 112, 115, 58, 47, 47, 99, 111, 115, 46, 101, 120, 97, 109, 112, 108,
        101, 46, 99, 111, 109, 18, 6, 98, 117, 99, 107, 101, 116, 26, 20, 47, 112, 97, 116, 104,
        47, 116, 111, 47, 115, 116, 97, 103, 101, 47, 102, 105, 108, 101, 115, 34, 9, 115, 101, 99,
        114, 101, 116, 95, 105, 100, 42, 10, 115, 101, 99, 114, 101, 116, 95, 107, 101, 121, 50,
        19, 8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 48, 6, 160, 6, 128, 1, 168, 6, 24, 160, 6, 128, 1,
        168, 6, 24,
    ];

    let want = || StorageCosConfig {
        endpoint_url: "https://cos.example.com".to_string(),
        root: "/path/to/stage/files".to_string(),
        secret_id: "secret_id".to_string(),
        secret_key: "secret_key".to_string(),
        bucket: "bucket".to_string(),
        network_config: Some(StorageNetworkParams {
            retry_timeout: 1,
            retry_io_timeout: 2,
            tcp_keepalive: 3,
            connect_timeout: 4,
            pool_max_idle_per_host: 5,
            max_concurrent_io_requests: 6,
        }),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        storage_cos_config_v128.as_slice(),
        128,
        want(),
    )?;

    Ok(())
}
