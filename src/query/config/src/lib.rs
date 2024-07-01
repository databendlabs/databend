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
#![feature(no_sanitize)]
#![feature(lazy_cell)]

mod background_config;
/// Config mods provide config support.
///
/// We are providing two config types:
///
/// - [`config::Config`] represents the options from command line , configuration files or environment vars.
/// - [`inner::InnerConfig`] "internal representation" of application settings, built from Config.
/// - [`global::GlobalConfig`] A global singleton of [`crate::InnerConfig`].
///
/// It's safe to refactor [`inner::InnerConfig`] in anyway, as long as it satisfied the following traits
///
/// - `TryInto<inner::InnerConfig> for config::Config`
/// - `From<inner::InnerConfig> for config::Config`
mod config;
mod global;
mod idm;
mod inner;
mod mask;
mod obsolete;
mod version;

pub use config::CacheStorageTypeConfig;
pub use config::Commands;
pub use config::Config;
pub use config::QueryConfig;
pub use config::StorageConfig;
pub use global::GlobalConfig;
pub use idm::*;
pub use inner::CacheConfig;
pub use inner::CacheStorageTypeConfig as CacheStorageTypeInnerConfig;
pub use inner::CatalogConfig;
pub use inner::CatalogHiveConfig;
pub use inner::DiskCacheKeyReloadPolicy;
pub use inner::InnerConfig;
pub use inner::ThriftProtocol;
pub use version::DATABEND_COMMIT_VERSION;
pub use version::QUERY_GIT_SEMVER;
pub use version::QUERY_GIT_SHA;
pub use version::QUERY_SEMVER;
