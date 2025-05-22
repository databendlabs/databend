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

use databend_common_meta_app::storage::StorageHuggingfaceConfig;
use databend_common_meta_app::storage::StorageNetworkParams;
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
    let storage_network_config_v128 = vec![
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
        storage_network_config_v128.as_slice(),
        128,
        want(),
    )?;
    Ok(())
}
