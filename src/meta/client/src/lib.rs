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

#![allow(clippy::uninlined_format_args)]

extern crate core;

mod channel_manager;
mod client_handle;
pub mod endpoints;
pub mod errors;
pub(crate) mod established_client;
mod grpc_action;
mod grpc_client;
mod grpc_metrics;
mod kv_api_impl;
mod message;
pub mod required;

use std::sync::LazyLock;

pub use channel_manager::MetaChannelManager;
pub use client_handle::ClientHandle;
pub use databend_common_meta_api::reply::reply_to_api_result;
use databend_common_version::DATABEND_GIT_SEMVER;
pub use grpc_action::MetaGrpcReadReq;
pub use grpc_action::MetaGrpcReq;
pub use grpc_action::RequestFor;
pub use grpc_client::MetaGrpcClient;
pub use message::ClientWorkerRequest;
pub use message::Streamed;
pub use required::FeatureSpec;
pub use required::VersionTuple;
use semver::Version;

pub static METACLI_COMMIT_SEMVER: LazyLock<Version> = LazyLock::new(|| {
    let semver = DATABEND_GIT_SEMVER.expect("DATABEND_GIT_SEMVER can not be None");
    let semver = semver.strip_prefix('v').unwrap_or(semver);

    Version::parse(semver).expect(
        "run `git fetch --tags` to solve this error,
    to learn more about this error, please visit https://crates.io/crates/semver",
    )
});

/// Oldest compatible nightly metasrv version
///
/// - 2022-10-19: after 0.8.79:
///   Update min compatible server to 0.8.35:
///   Since which, meta-server adds new API kv_api() to replace write_msg() and read_msg();
///   Feature commit: 69a05aca41036976fec37ad7a8b447e2868ef08b 2022-09-14
///
/// - 2023-02-04: since 0.9.24:
///   Remove read_msg and write_msg from service definition meta.proto
///   Update server.min_cli_ver to 0.8.80, the min ver in which meta-client switched from
///   `read_msg/write_msg` to `kv_api`
///
/// - 2023-02-16: since 0.9.41:
///   👥 client add `Compatible` layer to accept KVAppError or MetaAPIError
///
/// - 2023-02-17: since 0.9.42:
///   🖥 server only responds with MetaAPIError.
///
/// - 2023-05-07: since 1.1.32:
///   🖥 server: add: TxnDeleteRequest provides a `match_seq` field to delete a record if its `seq` matches.
///
/// - 2023-10-11: since 1.2.153:
///   🖥 server: add: pb::SeqV.meta field to support record expiration.
///
/// - 2023-10-17: since 1.2.163:
///   🖥 server: add: stream api: kv_read_v1().
///
/// - 2023-10-20: since 1.2.176:
///   👥 client: call stream api: kv_read_v1(), revert to 1.1.32 if server < 1.2.163
///
/// - 2023-12-16: since 1.2.258:
///   🖥 server: add: ttl to TxnPutRequest and Upsert
///
/// - 2024-01-02: since 1.2.279:
///   👥 client: remove `Compatible` for KVAppError and MetaAPIError, added in `2023-02-16: since 0.9.41`
///
/// - 2024-01-07: since 1.2.287:
///   👥 client: remove calling RPC kv_api() with MetaGrpcReq::GetKV/MGetKV/ListKV, kv_api only accept Upsert;
///   👥 client: remove using MetaGrpcReq::GetKV/MGetKV/ListKV;
///   👥 client: remove falling back kv_read_v1(Streamed(List)) to kv_api(List), added in `2023-10-20: since 1.2.176`;
///
/// - 2024-01-17: since 1.2.304:
///   🖥 server: do not use TxnPutRequest.prev_value;
///   🖥 server: do not use TxnDeleteRequest.prev_value;
///   Always return the previous value;
///   field index is reserved, no compatibility changes.
///
/// - 2024-01-25: since 1.2.315:
///   🖥 server: add export_v1() to let client specify export chunk size;
///
/// - 2024-03-01: since: 1.2.358:
///   🖥 server: add `server_time` to `get_client_info() -> ClientInfo`,
///
/// - 2024-03-04: since: 1.2.361
///   👥 client: `MetaSpec` use `ttl`, remove `expire_at`, require 1.2.258
///
/// - 2024-11-22: since 1.2.663
///   🖥 server: remove `MetaGrpcReq::GetKV/MGetKV/ListKV`,
///   require the client to call kv_read_v1 for get/mget/list,
///   which is added `2024-01-07: since 1.2.287`
///
/// - 2024-11-23: since 1.2.663
///   👥 client: remove use of `Operation::AsIs`
///
/// - 2024-12-16: since 1.2.674
///   🖥 server: add `txn_condition::Target::KeysWithPrefix`,
///   to support matching the key count by a prefix.
///
/// - 2024-12-20: since 1.2.676
///   🖥 server: add `TxnRequest::operations`,
///   to specify a complex bool expression and corresponding operations
///
/// - 2024-12-26: since 1.2.677
///   🖥 server: add `WatchRequest::initial_flush`,
///   to let watch stream flush all keys in a range at the beginning.
///
/// - 2025-03-30: since 1.2.715
///   👥 client: semaphore(watch) requires `WatchRequest::initial_flush`(`1,2.677`),
///   other RPC does not require `1.2.677`, requires only `1.2.259`.
///
/// - 2025-04-15: since TODO: add version when merged.
///   👥 client: requires `1,2.677`.
///
///
/// Server feature set:
/// ```yaml
/// server_features:
///   txn_delete_match_seq: ["2023-05-07", "1.1.32", ]
///   pb_seqv_meta:         ["2023-10-11", "1.2.153", ]
///   kv_read_v1:           ["2023-10-17", "1.2.163", ]
/// ```
// ------------------------------
// The binary in the https://github.com/datafuselabs/databend/releases/tag/v1.2.258-nightly
// outputs version 1.2.257;
// ```
// ./databend-meta  --single
// Databend Metasrv
// Version: v1.2.257-nightly-188426e3e6-simd(1.75.0-nightly-2023-12-17T22:09:06.675156000Z)
// ```
// Skip 1.2.258 use the next 1.2.259
pub static MIN_METASRV_SEMVER: Version = Version::new(1, 2, 677);

pub fn to_digit_ver(v: &Version) -> u64 {
    v.major * 1_000_000 + v.minor * 1_000 + v.patch
}

pub fn from_digit_ver(u: u64) -> Version {
    Version::new(u / 1_000_000, u / 1_000 % 1_000, u % 1_000)
}
