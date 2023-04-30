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

//! This mod defines on-disk data version, the storage of the data version, and provide upgrade functions.

mod data_version;
mod header;
use std::fmt;

use common_meta_sled_store::sled;
use common_meta_sled_store::SledTree;
use common_meta_stoerr::MetaStorageError;
pub use data_version::DataVersion;
pub use header::Header;
use tracing::info;

use crate::config::RaftConfig;
use crate::key_spaces::DataHeader;

/// The sled tree name to store the data versions.
pub const TREE_HEADER: &str = "header";

/// The data version the program runs on
pub static DATA_VERSION: DataVersion = DataVersion::V0;

/// On disk data descriptor.
///
/// It should be loaded before accessing other data on disk.
/// And if the data is upgrading, it should be upgraded before accessing other data on disk.
/// If the on disk data is an old version, it should be upgraded to the current version.
#[derive(Debug, Clone)]
pub struct OnDisk {
    pub header: Header,

    #[allow(dead_code)]
    db: sled::Db,

    config: RaftConfig,
}

impl fmt::Display for OnDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "header: {:?}, data-dir: {}",
            self.header, self.config.raft_dir
        )
    }
}

impl OnDisk {
    /// Initialize data version for local store, returns the loaded version.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn open(db: &sled::Db, config: &RaftConfig) -> Result<OnDisk, MetaStorageError> {
        info!(?config, "open and initialize data-version");

        const KEY_HEADER: &str = "header";

        let tree_name = config.tree_name(TREE_HEADER);
        let tree = SledTree::open(db, &tree_name, config.is_sync())?;
        let ks = tree.key_space::<DataHeader>();

        let header = ks.get(&KEY_HEADER.to_string())?;
        info!("Loaded header: {:?}", header);

        if let Some(v) = header {
            return Ok(OnDisk {
                header: v,
                db: db.clone(),
                config: config.clone(),
            });
        }

        let header = Header {
            version: DATA_VERSION,
            upgrading: None,
        };
        ks.insert(&KEY_HEADER.to_string(), &header).await?;

        Ok(OnDisk {
            header,
            db: db.clone(),
            config: config.clone(),
        })
    }

    /// Upgrade the on-disk data to latest version `DATA_VERSION`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn upgrade(&mut self) -> Result<(), MetaStorageError> {
        if let Some(u) = self.header.upgrading {
            let _ = u;
            // Currently there is no other version
            // TODO: discard unfinished upgrading data and redo.
        }

        #[allow(clippy::while_immutable_condition)]
        while self.header.version != DATA_VERSION {
            match self.header.version {
                DataVersion::V0 => {
                    // TODO: upgrade compat::07 data
                }
            }
        }

        Ok(())
    }
}
