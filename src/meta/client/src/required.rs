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

//! Defines the feature this client requires from the databend-meta server.

use std::collections::BTreeMap;

pub type VersionTuple = (u64, u64, u64);

pub type FeatureSpec = (&'static str, VersionTuple);

pub type Features = BTreeMap<&'static str, VersionTuple>;

#[rustfmt::skip]
pub mod features {
    use crate::FeatureSpec;

    pub const KV_API:               FeatureSpec = ("kv_api",               (1, 2, 259));
    pub const KV_READ_V1:           FeatureSpec = ("kv_read_v1",           (1, 2, 259));
    pub const TRANSACTION:          FeatureSpec = ("transaction",          (1, 2, 259));
    pub const EXPORT:               FeatureSpec = ("export",               (1, 2, 259));
    // `export_v1` is not a required feature because the client fallback to `export` if `export_v1` is not available.
    pub const EXPORT_V1:            FeatureSpec = ("export_v1",            (1, 2, 315));
    pub const WATCH:                FeatureSpec = ("watch",                (1, 2, 259));
    pub const WATCH_INITIAL_FLUSH:  FeatureSpec = ("watch/initial_flush",  (1, 2, 677));
    /// WatchResponse contains a flag to indicate whether the event is initialization event or change event.
    pub const WATCH_INIT_FLAG:      FeatureSpec = ("watch/init_flag",      (1, 2, 736));
    pub const MEMBER_LIST:          FeatureSpec = ("member_list",          (1, 2, 259));
    pub const GET_CLUSTER_STATUS:   FeatureSpec = ("get_cluster_status",   (1, 2, 259));
    pub const GET_CLIENT_INFO:      FeatureSpec = ("get_client_info",      (1, 2, 259));
    /// TxnPutResponse contains the `current` state of the key after the put operation.
    pub const PUT_RESPONSE_CURRENT: FeatureSpec = ("put_response/current", (1, 2, 756));
    /// Txn support a FetchAdd operation for json encoded u64 values.
    pub const FETCH_ADD_U64:        FeatureSpec = ("fetch_add_u64",        (1, 2, 764));
    /// - 2025-07-03: since 1.2.770
    ///   🖥 server: adaptive `expire_at` support both seconds and milliseconds.
    pub const EXPIRE_IN_MILLIS:     FeatureSpec = ("expire_in_millis",     (1, 2, 770));
    /// - 2025-07-04: since 1.2.770
    ///   🖥 server: add `PutSequential`.
    pub const PUT_SEQUENTIAL:       FeatureSpec = ("put_sequential",       (1, 2, 770));
    /// - 2025-09-30: since 1.2.823
    ///   🖥 server: store raft-log proposing time `proposed_at_ms` in `KVMeta`.
    pub const PROPOSED_AT_MS:       FeatureSpec = ("proposed_at_ms",       (1, 2, 823));
    /// - 2025-10-16: since 1.2.828
    ///   🖥 server: rename `FetchAddU64` to `FetchIncreaseU64`, add `max_value`.
    pub const FETCH_INCREASE_U64:   FeatureSpec = ("fetch_increase_u64",   (1, 2, 828));

}

/// All features that are ever defined and their least required server version.
pub fn all() -> &'static [FeatureSpec] {
    #[rustfmt::skip]
    pub const REQUIRES: &[FeatureSpec] = &[
        features::KV_API,
        features::KV_READ_V1,
        features::TRANSACTION,
        features::EXPORT,
        features::EXPORT_V1,
        features::WATCH,
        features::WATCH_INITIAL_FLUSH,
        features::WATCH_INIT_FLAG,
        features::MEMBER_LIST,
        features::GET_CLUSTER_STATUS,
        features::GET_CLIENT_INFO,
        features::PUT_RESPONSE_CURRENT,
        features::FETCH_ADD_U64,
        features::EXPIRE_IN_MILLIS,
        features::PUT_SEQUENTIAL,
        features::PROPOSED_AT_MS,
        features::FETCH_INCREASE_U64,
    ];

    REQUIRES
}

/// Filter out supported features by the given version.
pub fn supported_features(version: (u64, u64, u64)) -> BTreeMap<&'static str, VersionTuple> {
    all()
        .iter()
        .filter(|(_, v)| version >= *v)
        .copied()
        .collect()
}

/// Features a standard meta-client requires.
///
/// `export_v1` is not a required feature because the client fallback to `export` if `export_v1` is not available.
pub fn std() -> &'static [FeatureSpec] {
    #[rustfmt::skip]
    pub const REQUIRES: &[FeatureSpec] = &[
        features::KV_READ_V1,
        features::TRANSACTION,
        features::EXPORT,
        // `export_v1` is not a required feature
        // features::EXPORT_V1,
        features::WATCH,
        features::WATCH_INITIAL_FLUSH,
        // MIN_METASRV_VER does not include this feature, thus it is optional
        // features::WATCH_INIT_FLAG,
        features::MEMBER_LIST,
        features::GET_CLUSTER_STATUS,
        features::GET_CLIENT_INFO,
        // - 2025-09-27: since 1.2.823
        //   👥 client: require 1.2.770, remove calling RPC kv_api
        features::PUT_RESPONSE_CURRENT,
        features::FETCH_ADD_U64,
        features::EXPIRE_IN_MILLIS,
        features::PUT_SEQUENTIAL,
    ];

    REQUIRES
}

/// Features that are required by business logic, such as read, write and transaction
pub fn read_write() -> &'static [FeatureSpec] {
    #[rustfmt::skip]
    pub const REQUIRES: &[FeatureSpec] = &[
        features::KV_READ_V1,
        features::TRANSACTION,
        features::PUT_RESPONSE_CURRENT,
        features::FETCH_ADD_U64,
        features::EXPIRE_IN_MILLIS,
        features::PUT_SEQUENTIAL,
    ];

    REQUIRES
}

/// Features that are required by export api
///
/// A client that only needs to use the export api specifies these features.
pub fn export() -> &'static [FeatureSpec] {
    #[rustfmt::skip]
    pub const REQUIRES: &[FeatureSpec] = &[
        features::EXPORT,
        // `export_v1` is not a required feature
        // features::EXPORT_V1,
    ];

    REQUIRES
}
