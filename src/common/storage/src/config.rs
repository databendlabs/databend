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

use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_meta_app::storage::StorageParams;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::auth::RefreshableToken;
use crate::auth::TokenFile;

/// Config for storage backend.
///
/// # TODO(xuanwo)
///
/// In the future, we will use the following storage config layout:
///
/// ```toml
/// [storage]
///
/// [storage.data]
/// type = "s3"
///
/// [storage.temporary]
/// type = "s3"
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    pub num_cpus: u64,
    pub allow_insecure: bool,
    pub params: StorageParams,
    /// Global switches that affect the ambient credential chain behavior.
    ///
    /// Notes:
    /// - These are runtime-only controls and are not persisted in meta.
    /// - They apply to all storage operators created in this process.
    pub disable_config_load: bool,
    pub disable_instance_profile: bool,
}

/// Runtime-only switches for ambient credential chain behavior.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialChainConfig {
    pub disable_config_load: bool,
    pub disable_instance_profile: bool,
}

impl CredentialChainConfig {
    pub fn init(cfg: CredentialChainConfig) -> databend_common_exception::Result<()> {
        GlobalInstance::set(cfg);
        Ok(())
    }

    pub fn try_get() -> Option<CredentialChainConfig> {
        GlobalInstance::try_get()
    }
}

// TODO: This config should be moved out of common-storage crate.
#[derive(Clone)]
pub struct ShareTableConfig {
    pub share_endpoint_address: Option<String>,
    pub share_endpoint_token: RefreshableToken,
}

impl ShareTableConfig {
    pub fn init(
        share_endpoint_address: &str,
        token_file: &str,
        default_token: String,
    ) -> databend_common_exception::Result<()> {
        GlobalInstance::set(Self::try_create(
            share_endpoint_address,
            token_file,
            default_token,
        )?);

        Ok(())
    }

    pub fn try_create(
        share_endpoint_address: &str,
        token_file: &str,
        default_token: String,
    ) -> databend_common_exception::Result<ShareTableConfig> {
        let share_endpoint_address = if share_endpoint_address.is_empty() {
            None
        } else {
            Some(share_endpoint_address.to_owned())
        };
        let share_endpoint_token = if token_file.is_empty() {
            RefreshableToken::Direct(default_token)
        } else {
            let s = String::from(token_file);
            let f = TokenFile::new(Path::new(&s))?;
            RefreshableToken::File(Arc::new(RwLock::new(f)))
        };
        Ok(ShareTableConfig {
            share_endpoint_address,
            share_endpoint_token,
        })
    }

    pub fn share_endpoint_address() -> Option<String> {
        ShareTableConfig::instance().share_endpoint_address
    }

    pub fn share_endpoint_token() -> RefreshableToken {
        ShareTableConfig::instance().share_endpoint_token
    }

    pub fn instance() -> ShareTableConfig {
        GlobalInstance::get()
    }
}
