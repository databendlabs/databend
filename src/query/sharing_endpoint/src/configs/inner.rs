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

use databend_common_exception::Result;
use databend_common_storage::StorageConfig;

use super::outer_v0::Config as OuterV0Config;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub tenant: String,
    pub share_endpoint_address: String,
    pub storage: StorageConfig,
}

impl Config {
    /// As requires by [RFC: Config Backward Compatibility](https://github.com/datafuselabs/databend/pull/5324), we will load user's config via wrapper [`ConfigV0`] and then convert from [`ConfigV0`] to [`Config`].
    ///
    /// In the future, we could have `ConfigV1` and `ConfigV2`.
    pub async fn load() -> Result<Self> {
        let mut cfg: Self = OuterV0Config::load(true)?.try_into()?;
        cfg.storage.params = cfg.storage.params.auto_detect().await?;

        Ok(cfg)
    }

    /// # NOTE
    ///
    /// This function is served for tests only.
    pub fn load_for_test() -> Result<Self> {
        let cfg: Self = OuterV0Config::load(false)?.try_into()?;
        Ok(cfg)
    }

    /// Transform config into the outer style.
    ///
    /// This function should only be used for end-users.
    pub fn into_outer(self) -> OuterV0Config {
        OuterV0Config::from(self)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tenant: "".to_string(),
            share_endpoint_address: "".to_string(),
            storage: StorageConfig::default(),
        }
    }
}
