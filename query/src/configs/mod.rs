// Copyright 2020 Datafuse Labs.
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

#[macro_use]
mod macros;

#[cfg(test)]
mod config_test;

mod config;
pub mod config_log;
pub mod config_meta;
pub mod config_query;
pub mod config_storage;

pub use config::Config;
pub use config::DATABEND_COMMIT_VERSION;
pub use config_log::LogConfig;
pub use config_meta::MetaConfig;
pub use config_query::QueryConfig;
pub use config_storage::AzureStorageBlobConfig;
pub use config_storage::DiskStorageConfig;
pub use config_storage::S3StorageConfig;
pub use config_storage::StorageConfig;
