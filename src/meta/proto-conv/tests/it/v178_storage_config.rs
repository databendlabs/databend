// Copyright 2026 Datafuse Labs.
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

use databend_common_meta_app::storage::StorageAzblobConfig;
use databend_common_meta_app::storage::StorageFtpConfig;
use databend_common_meta_app::storage::StorageHttpConfig;
use databend_common_meta_app::storage::StorageIpfsConfig;
use databend_common_meta_app::storage::StorageNetworkParams;
use databend_common_meta_app::storage::StorageParams;
use fastrace::func_name;

use crate::common;

fn network_config() -> Option<StorageNetworkParams> {
    Some(StorageNetworkParams {
        retry_timeout: 1,
        retry_io_timeout: 2,
        tcp_keepalive: 3,
        connect_timeout: 4,
        pool_max_idle_per_host: 5,
        max_concurrent_io_requests: 6,
    })
}

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
#[test]
fn test_decode_v178_storage_config_azblob() -> anyhow::Result<()> {
    let storage_config_v178 = vec![
        82, 88, 10, 26, 104, 116, 116, 112, 115, 58, 47, 47, 97, 122, 98, 108, 111, 98, 46, 101,
        120, 97, 109, 112, 108, 101, 46, 99, 111, 109, 18, 9, 99, 111, 110, 116, 97, 105, 110, 101,
        114, 26, 7, 97, 99, 99, 111, 117, 110, 116, 34, 3, 107, 101, 121, 42, 5, 47, 100, 97, 116,
        97, 50, 19, 8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 48, 6, 160, 6, 178, 1, 168, 6, 24, 160, 6,
        178, 1, 168, 6, 24,
    ];

    let want = || {
        StorageParams::Azblob(StorageAzblobConfig {
            endpoint_url: "https://azblob.example.com".to_string(),
            container: "container".to_string(),
            account_name: "account".to_string(),
            account_key: "key".to_string(),
            root: "/data".to_string(),
            network_config: network_config(),
        })
    };

    // StorageConfig itself does not carry version fields and get_pb_ver() returns 0.
    common::test_load_old(func_name!(), storage_config_v178.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

#[test]
fn test_decode_v178_storage_config_ftp() -> anyhow::Result<()> {
    let storage_config_v178 = vec![
        90, 76, 10, 22, 102, 116, 112, 115, 58, 47, 47, 102, 116, 112, 46, 101, 120, 97, 109, 112,
        108, 101, 46, 99, 111, 109, 18, 6, 47, 102, 105, 108, 101, 115, 26, 4, 117, 115, 101, 114,
        34, 8, 112, 97, 115, 115, 119, 111, 114, 100, 42, 19, 8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 48,
        6, 160, 6, 178, 1, 168, 6, 24, 160, 6, 178, 1, 168, 6, 24,
    ];

    let want = || {
        StorageParams::Ftp(StorageFtpConfig {
            endpoint: "ftps://ftp.example.com".to_string(),
            root: "/files".to_string(),
            username: "user".to_string(),
            password: "password".to_string(),
            network_config: network_config(),
        })
    };

    common::test_load_old(func_name!(), storage_config_v178.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

#[test]
fn test_decode_v178_storage_config_http() -> anyhow::Result<()> {
    let storage_config_v178 = vec![
        98, 70, 10, 24, 104, 116, 116, 112, 115, 58, 47, 47, 100, 97, 116, 97, 46, 101, 120, 97,
        109, 112, 108, 101, 46, 99, 111, 109, 18, 6, 47, 97, 46, 99, 115, 118, 18, 6, 47, 98, 46,
        99, 115, 118, 26, 19, 8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 48, 6, 160, 6, 178, 1, 168, 6, 24,
        160, 6, 178, 1, 168, 6, 24,
    ];

    let want = || {
        StorageParams::Http(StorageHttpConfig {
            endpoint_url: "https://data.example.com".to_string(),
            paths: vec!["/a.csv".to_string(), "/b.csv".to_string()],
            network_config: network_config(),
        })
    };

    common::test_load_old(func_name!(), storage_config_v178.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

#[test]
fn test_decode_v178_storage_config_ipfs() -> anyhow::Result<()> {
    let storage_config_v178 = vec![
        106, 66, 10, 24, 104, 116, 116, 112, 115, 58, 47, 47, 105, 112, 102, 115, 46, 101, 120, 97,
        109, 112, 108, 101, 46, 99, 111, 109, 18, 10, 47, 105, 112, 102, 115, 47, 114, 111, 111,
        116, 26, 19, 8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 48, 6, 160, 6, 178, 1, 168, 6, 24, 160, 6,
        178, 1, 168, 6, 24,
    ];

    let want = || {
        StorageParams::Ipfs(StorageIpfsConfig {
            endpoint_url: "https://ipfs.example.com".to_string(),
            root: "/ipfs/root".to_string(),
            network_config: network_config(),
        })
    };

    common::test_load_old(func_name!(), storage_config_v178.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

#[test]
fn test_decode_v178_storage_config_memory() -> anyhow::Result<()> {
    let storage_config_v178 = vec![114, 7, 160, 6, 178, 1, 168, 6, 24];

    let want = || StorageParams::Memory;

    common::test_load_old(func_name!(), storage_config_v178.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
