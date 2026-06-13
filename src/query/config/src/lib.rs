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
#![allow(clippy::derivable_impls)]

mod builtin;

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
mod inner;
mod mask;
mod obsolete;
mod toml;
pub use builtin::*;
pub use config::CacheStorageTypeConfig;
pub use config::Commands;
pub use config::Config;
pub use config::StorageConfig;
pub use config::StorageNetworkConfig;
pub use global::GlobalConfig;
pub use inner::CacheConfig;
pub use inner::CacheStorageTypeConfig as CacheStorageTypeInnerConfig;
pub use inner::CatalogConfig;
pub use inner::CatalogHiveConfig;
pub use inner::DiskCacheConfig as DiskCacheInnerConfig;
pub use inner::DiskCacheKeyReloadPolicy;
pub use inner::InnerConfig;
pub use inner::MetaConfig;
pub use inner::QueryConfig;
pub use inner::SpillConfig;
pub use inner::ThriftProtocol;
