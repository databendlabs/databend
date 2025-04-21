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

    pub const KV_API:              FeatureSpec = ("kv_api",              (1, 2, 259));
    pub const KV_READ_V1:          FeatureSpec = ("kv_read_v1",          (1, 2, 259));
    pub const TRANSACTION:         FeatureSpec = ("transaction",         (1, 2, 259));
    pub const EXPORT:              FeatureSpec = ("export",              (1, 2, 259));
    // `export_v1` is not a required feature because the client fallback to `export` if `export_v1` is not available.
    pub const EXPORT_V1:           FeatureSpec = ("export_v1",           (1, 2, 315));
    pub const WATCH:               FeatureSpec = ("watch",               (1, 2, 259));
    pub const WATCH_INITIAL_FLUSH: FeatureSpec = ("watch/initial_flush", (1, 2, 677));
    pub const MEMBER_LIST:         FeatureSpec = ("member_list",         (1, 2, 259));
    pub const GET_CLUSTER_STATUS:  FeatureSpec = ("get_cluster_status",  (1, 2, 259));
    pub const GET_CLIENT_INFO:     FeatureSpec = ("get_client_info",     (1, 2, 259));
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
        features::MEMBER_LIST,
        features::GET_CLUSTER_STATUS,
        features::GET_CLIENT_INFO,
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
        features::KV_API,
        features::KV_READ_V1,
        features::TRANSACTION,
        features::EXPORT,
        // `export_v1` is not a required feature
        // features::EXPORT_V1,
        features::WATCH,
        features::WATCH_INITIAL_FLUSH,
        features::MEMBER_LIST,
        features::GET_CLUSTER_STATUS,
        features::GET_CLIENT_INFO,
    ];

    REQUIRES
}

/// Features that are required by business logic, such as read, write and transaction
pub fn read_write() -> &'static [FeatureSpec] {
    #[rustfmt::skip]
    pub const REQUIRES: &[FeatureSpec] = &[
        features::KV_API,
        features::KV_READ_V1,
        features::TRANSACTION,
    ];

    REQUIRES
}

/// Features that are required by business logic, such as read, write and transaction and watch
pub fn read_write_watch() -> &'static [FeatureSpec] {
    #[rustfmt::skip]
    pub const REQUIRES: &[FeatureSpec] = &[
        features::KV_API,
        features::KV_READ_V1,
        features::TRANSACTION,
        features::WATCH,
        features::WATCH_INITIAL_FLUSH,
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
