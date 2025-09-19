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
pub(crate) mod upgrade_to_v004;
pub(crate) mod version_info;

use std::fmt;
use std::fmt::Debug;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

pub use data_version::DataVersion;
use databend_common_meta_sled_store::init_get_sled_db;
use databend_common_meta_sled_store::SledTree;
use databend_common_meta_stoerr::MetaStorageError;
pub use header::Header;
use log::info;
use raft_log::codeq::error_context_ext::ErrorContextExt;

use crate::config::RaftConfig;
use crate::key_spaces::DataHeader;

/// The sled tree name to store the data versions.
pub const TREE_HEADER: &str = "header";

/// The working data version the program runs on
pub static DATA_VERSION: DataVersion = DataVersion::V004;

/// On disk data descriptor.
///
/// It should be loaded before accessing other data on disk.
/// And if the data is upgrading, it should be upgraded before accessing other data on disk.
/// If the on disk data is an old version, it should be upgraded to the current version.
#[derive(Debug, Clone)]
pub struct OnDisk {
    pub header: Header,

    config: RaftConfig,

    log_stderr: bool,
}

impl fmt::Display for OnDisk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "header: {:?}, data-dir: {}",
            self.header, self.config.raft_dir
        )
    }
}

impl OnDisk {
    pub const KEY_HEADER: &'static str = "header";

    pub fn ensure_dirs(raft_dir: &str) -> Result<(), io::Error> {
        let raft_dir = Path::new(raft_dir);
        let version_dir = raft_dir.join("df_meta").join(format!("{}", DATA_VERSION));

        let log_dir = version_dir.join("log");
        if !log_dir.exists() {
            fs::create_dir_all(&log_dir)
                .context(|| format!("creating dir {}", log_dir.as_path().display()))?;
            info!("Created log dir: {}", log_dir.as_path().display());
        }

        let snapshot_dir = version_dir.join("snapshot");
        if !snapshot_dir.exists() {
            fs::create_dir_all(&snapshot_dir)
                .context(|| format!("creating dir {}", snapshot_dir.as_path().display()))?;
            info!("Created snapshot dir: {}", snapshot_dir.as_path().display());
        }

        Ok(())
    }

    /// Initialize data version for local store, returns the loaded version.
    #[fastrace::trace]
    pub async fn open(config: &RaftConfig) -> Result<OnDisk, MetaStorageError> {
        info!(config :? =(config); "open and initialize data-version");

        Self::ensure_dirs(&config.raft_dir)?;

        Self::upgrade_header(config).await?;

        let header = Self::load_header_from_fs(config)?;
        info!("Loaded header from fs: {:?}", header);

        if let Some(v) = header {
            return Ok(OnDisk::new(v, config));
        }

        // Without header, by default it is the oldest compatible version: V003.

        let header = Header {
            version: DataVersion::V003,
            upgrading: None,
            cleaning: false,
        };

        Self::write_header_to_fs(config, &header)?;

        Ok(OnDisk::new(header, config))
    }

    pub fn new(header: Header, config: &RaftConfig) -> Self {
        let min_compatible = DATA_VERSION.min_compatible_data_version();

        if header.version < min_compatible {
            let max_compatible_working_version = header.version.max_compatible_working_version();
            let version_info = min_compatible.version_info();

            eprintln!("Working data version is: {}", DATA_VERSION);
            eprintln!("On-disk data version is too old: {}", header.version);
            eprintln!(
                "The latest compatible version is {}",
                max_compatible_working_version
            );
            eprintln!(
                "Download the latest compatible version: {}",
                version_info.download_url()
            );

            panic!(
                "On-disk data version {} is too old, the latest compatible version is {}.",
                header.version, max_compatible_working_version
            );
        }

        Self {
            header,
            config: config.clone(),
            log_stderr: false,
        }
    }

    async fn upgrade_header(config: &RaftConfig) -> Result<(), io::Error> {
        let header_path = Self::header_path(config);
        if header_path.exists() {
            info!("Header file exists, no need to upgrade");
            return Ok(());
        }

        let db = init_get_sled_db(config.raft_dir.clone(), 1024 * 1024 * 1024);

        let tree = SledTree::open(&db, TREE_HEADER)?;
        let ks = tree.key_space::<DataHeader>();

        let header = ks.get(&Self::KEY_HEADER.to_string()).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e).context(|| "open on-disk data")
        })?;
        info!("Found and loaded header from sled: {:?}", header);

        if let Some(header) = header {
            Self::write_header_to_fs(config, &header)?;

            ks.remove_no_return(&Self::KEY_HEADER.to_string(), true)
                .await
                .map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, e)
                        .context(|| "remove header from sled")
                })?;

            info!("Removed header from sled");
        }

        Ok(())
    }

    fn header_path(config: &RaftConfig) -> PathBuf {
        let raft_dir = Path::new(&config.raft_dir);
        raft_dir.join("df_meta").join("VERSION")
    }

    pub(crate) fn write_header_to_fs(
        config: &RaftConfig,
        header: &Header,
    ) -> Result<(), io::Error> {
        let header_path = Self::header_path(config);
        let buf = serde_json::to_vec(header).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
                .context(|| format!("serializing header at {}", header_path.as_path().display(),))
        })?;

        fs::write(&header_path, &buf).context(|| {
            format!(
                "writing version file at {}: {}",
                header_path.as_path().display(),
                String::from_utf8_lossy(&buf)
            )
        })?;

        info!(
            "Wrote header {:?}; at {}",
            header,
            header_path.as_path().display()
        );

        Ok(())
    }

    pub(crate) fn load_header_from_fs(config: &RaftConfig) -> Result<Option<Header>, io::Error> {
        let header_path = Self::header_path(config);

        if !header_path.exists() {
            return Ok(None);
        }

        let state = fs::read(&header_path)
            .context(|| format!("reading version file {}", header_path.as_path().display(),))?;

        let state = serde_json::from_slice::<Header>(&state).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
                .context(|| format!("parsing version file {}", header_path.as_path().display(),))
        })?;

        Ok(Some(state))
    }

    /// Enable or disable logging crucial steps to stderr, when upgrading.
    pub fn log_stderr(&mut self, log_stderr: bool) {
        self.log_stderr = log_stderr;
    }

    /// Upgrade the on-disk data to latest version `DATA_VERSION`.
    #[fastrace::trace]
    pub async fn upgrade(&mut self) -> Result<(), MetaStorageError> {
        self.progress(format_args!(
            "Upgrade ondisk data if out of date: {}",
            self.header
        ));

        self.progress(format_args!(
            "    Find and clean previous unfinished upgrading",
        ));

        if let Some(u) = self.header.upgrading {
            self.progress(format_args!("Found unfinished upgrading: {:?}", u));

            match u {
                DataVersion::V0 => {
                    unreachable!("Upgrading to V0 is not supported");
                }
                DataVersion::V001 => {
                    unreachable!("Upgrading V0 to V001 is not supported since 2024-06-13, 1.2.528");
                }
                DataVersion::V002 => {
                    unreachable!(
                        "Upgrading V001 to V002 is not supported since 2024-06-13, 1.2.528"
                    );
                }
                DataVersion::V003 => {
                    unreachable!(
                        "Upgrading V002 to V003 is not supported since 2025-07-01, TODO version"
                    );
                }
                DataVersion::V004 => {
                    self.clean_in_progress_v003_to_v004().await?;
                }
            }

            self.header.upgrading = None;
            self.write_header(&self.header)?;
            self.progress(format_args!("Cleared upgrading flag"));
        }

        #[allow(clippy::never_loop)]
        #[allow(clippy::while_immutable_condition)]
        while self.header.version != DATA_VERSION {
            match self.header.version {
                DataVersion::V0 => {
                    unreachable!(
                        "{} is no longer supported, since 2024-03-01",
                        self.header.version
                    )
                }
                DataVersion::V001 => {
                    unreachable!(
                        "{} is no longer supported, since 2024-06-13, 1.2.528",
                        self.header.version
                    )
                }
                DataVersion::V002 => {
                    unreachable!(
                        "{} is no longer supported, since 2025-07-01, TODO version",
                        self.header.version
                    )
                }
                DataVersion::V003 => {
                    self.upgrade_v003_to_v004().await?;
                }
                DataVersion::V004 => {
                    unreachable!("{} is the latest version", self.header.version)
                }
            }
        }

        self.progress(format_args!(
            "Upgrade ondisk data finished: {}",
            self.header
        ));

        Ok(())
    }

    /// Set upgrading flag indicating the upgrading is in progress.
    ///
    /// When it crashes before upgrading finishes, it can redo the upgrading.
    #[allow(dead_code)]
    async fn begin_upgrading(&mut self, from_ver: DataVersion) -> Result<(), MetaStorageError> {
        assert_eq!(from_ver, self.header.version);

        let next = self.header.version.next().unwrap();

        self.progress(format_args!("Upgrade on-disk data"));
        self.progress(format_args!("    From: {:?}", self.header.version));
        self.progress(format_args!("    To:   {:?}", next));

        assert!(self.header.upgrading.is_none(), "can not upgrade twice");

        self.header.upgrading = self.header.version.next();

        self.write_header(&self.header)?;
        Ok(())
    }

    fn clean_upgrading(&mut self) -> Result<(), io::Error> {
        assert!(self.header.upgrading.is_some());

        self.header.cleaning = true;
        self.progress(format_args!("    Clean upgrading: {}", self.header));

        self.write_header(&self.header)?;
        Ok(())
    }

    /// Reset upgrading flag indicating the upgrading is finished, and set header.version to next version.
    fn finish_upgrading(&mut self) -> Result<(), MetaStorageError> {
        self.header.version = self.header.upgrading.unwrap();
        self.header.upgrading = None;
        self.header.cleaning = false;
        self.progress(format_args!("    Finished upgrading: {}", self.header));

        self.write_header(&self.header)?;
        Ok(())
    }

    pub fn write_header(&self, header: &Header) -> Result<(), MetaStorageError> {
        Self::write_header_to_fs(&self.config, header)?;
        Ok(())
    }

    fn progress(&self, s: impl fmt::Display) {
        if self.log_stderr {
            eprintln!("{}", s);
        }

        info!("{}", s);
    }
}
