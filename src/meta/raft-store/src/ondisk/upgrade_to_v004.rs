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

//! Provide upgrading to v003 and cleaning v002

use std::fs;
use std::path::Path;
use std::sync::Arc;

use databend_common_meta_runtime_api::SpawnApi;
use databend_common_meta_sled_store::SledTree;
use databend_common_meta_sled_store::drop_sled_db;
use databend_common_meta_sled_store::init_get_sled_db;
use databend_common_meta_stoerr::MetaStorageError;
use fs_extra::dir::CopyOptions;
use log::debug;
use openraft::LogIdOptionExt;
use raft_log::codeq::error_context_ext::ErrorContextExt;
use tokio::io;

use crate::key_spaces::LogMeta;
use crate::key_spaces::RaftStoreEntry;
use crate::ondisk::DataVersion;
use crate::ondisk::OnDisk;
use crate::raft_log_v004::RaftLogV004;
use crate::raft_log_v004::importer;
use crate::sm_v003::SnapshotStoreV003;
use crate::sm_v003::SnapshotStoreV004;
use crate::state_machine::LogMetaKey;

impl OnDisk {
    /// Upgrade the on-disk data form [`DataVersion::V003`] to [`DataVersion::V004`].
    ///
    /// `V003` saves log in sled db.
    /// `V004` saves log in WAL based raft log.
    #[fastrace::trace]
    pub(crate) async fn upgrade_v003_to_v004<SP: SpawnApi>(&mut self) -> Result<(), io::Error> {
        // The previous cleaning step may remove the dir
        Self::ensure_dirs(&self.config.raft_dir)?;

        self.begin_upgrading(DataVersion::V003).await?;

        // 1.1. upgrade raft log

        self.progress(format_args!("    Upgrade V003 raft log in sled db to V004"));

        let raft_log_config = self.config.clone().to_raft_log_config();
        let raft_log_config = Arc::new(raft_log_config);
        let raft_log = RaftLogV004::open(raft_log_config)?;
        let mut importer = importer::Importer::new(raft_log);

        let db = init_get_sled_db(self.config.raft_dir.clone(), 1024 * 1024 * 1024);

        // Read the purged index
        let first_log_index = {
            let tree = SledTree::open(&db, "raft_log")?;
            let ks = tree.key_space::<LogMeta>();
            let purged = ks.get(&LogMetaKey::LastPurged).map_err(|e| {
                io::Error::other(format!(
                    "{}; when(get last purged index from sled db for upgrading V003 to V004)",
                    e
                ))
            })?;

            purged.map(|v| v.log_id()).next_index()
        };

        // import logs
        {
            let tree = SledTree::open(&db, "raft_log")?;
            let it = tree.tree.iter();

            for (i, rkv) in it.enumerate() {
                let (k, v) = rkv.map_err(|e| {
                    io::Error::other(format!(
                        "{}; when(iterating raft log in sled db for upgrading V003 to V004)",
                        e
                    ))
                })?;

                let ent = RaftStoreEntry::deserialize(&k, &v)?;
                let upgraded = ent.upgrade();
                if let RaftStoreEntry::LogEntry(entry) = &upgraded {
                    if entry.log_id.index < first_log_index {
                        debug!(
                            "skip already purged log: {} when:(import V003 log to V004)",
                            entry.log_id
                        );
                        continue;
                    }
                }

                debug!("import upgraded V003 entry: {:?}", upgraded);
                importer.import_raft_store_entry(upgraded)?;

                if i % 5000 == 0 {
                    self.progress(format_args!("        Imported {} logs", i));
                }
            }
        }

        // import raft_state
        {
            let tree = SledTree::open(&db, "raft_state")?;
            let kvs = tree.export()?;
            for kv in kvs {
                let ent = RaftStoreEntry::deserialize(&kv[0], &kv[1])?;
                debug!("import V003 entry: {:?}", ent);
                let upgraded = ent.upgrade();
                importer.import_raft_store_entry(upgraded)?;
            }
        }

        importer.flush().await?;

        // 1.2. copy snapshot

        let ss_store_v003: SnapshotStoreV003<SP> = SnapshotStoreV003::new(self.config.clone());
        let ss_store_v004: SnapshotStoreV004<SP> = SnapshotStoreV004::new(self.config.clone());

        let v003_path = ss_store_v003.snapshot_config().snapshot_dir();
        let v004_path = ss_store_v004.snapshot_config().version_dir();

        if fs::metadata(&v003_path).is_ok() {
            let options = CopyOptions::new().overwrite(true).copy_inside(true);

            fs_extra::dir::copy(&v003_path, &v004_path, &options).map_err(|e| {
                io::Error::other(format!(
                    "{}; when(copy snapshot from {} to {})",
                    e, v003_path, v004_path,
                ))
            })?;
        }

        // 2. clean up old version data

        self.clean_upgrading()?;

        self.remove_v003_logs().await?;
        self.remove_v003_snapshot::<SP>().await?;

        // 3. finish upgrading

        self.finish_upgrading()?;

        Ok(())
    }

    /// Revert or finish the unfinished upgrade to v003.
    pub(crate) async fn clean_in_progress_v003_to_v004<SP: SpawnApi>(
        &mut self,
    ) -> Result<(), MetaStorageError> {
        assert!(self.header.upgrading.is_some());
        if self.header.cleaning {
            self.remove_v003_logs().await?;
            self.remove_v003_snapshot::<SP>().await?;

            // Note that this will increase `header.version`.
            self.finish_upgrading()?;
        } else {
            self.progress(format_args!("to V004 upgrade is in progress; Clean it"));

            let raft_dir = self.config.raft_dir.clone();
            let raft_dir = Path::new(&raft_dir);

            let p = raft_dir.join("df_meta").join("V004");

            fs::remove_dir_all(&p)
                .context(|| format!("remove unfinished upgrade in {}", p.as_path().display()))?;
        }

        Ok(())
    }

    async fn remove_v003_snapshot<SP: SpawnApi>(&mut self) -> Result<(), io::Error> {
        let ss_store_v003: SnapshotStoreV003<SP> = SnapshotStoreV003::new(self.config.clone());

        let v003_path = ss_store_v003.snapshot_config().snapshot_dir();

        if fs::metadata(&v003_path).is_ok() {
            fs::remove_dir_all(&v003_path).map_err(|e| {
                io::Error::other(format!("{}; when(remove V003 snapshot: {})", e, v003_path,))
            })?;
        }

        Ok(())
    }

    /// It removes the data from sled db.
    /// But not the sled db itself.
    async fn remove_v003_logs(&mut self) -> Result<(), MetaStorageError> {
        // After upgrading, no sled db is required.

        self.progress(format_args!("    Remove V003 log from sled db",));

        let db = init_get_sled_db(self.config.raft_dir.clone(), 1024 * 1024 * 1024);
        for tree_name in db.tree_names() {
            if tree_name == "__sled__default" {
                continue;
            }

            self.progress(format_args!(
                "        Removing sled tree: {}",
                String::from_utf8_lossy(&tree_name)
            ));

            db.drop_tree(&tree_name)
                .map_err(|e| {
                    io::Error::other(
                        format!(
                            "{}; when(drop sled tree: {})as_str; when(clear sled db after upgrading V003 to V004)",
                            e,
                            String::from_utf8_lossy(&tree_name)
                        ),
                    )
                })
                ?;
        }

        self.progress(format_args!("    Done: Remove V003 log from sled db",));

        drop_sled_db();

        // Sleep a while to make sure the sled db is dropped and released in the underlying file system.
        // There is an issue without sleep:
        // Error: std::io::error::Error: std::io::error::Error: Device or resource busy (os error 16); when:(remove dir /var/lib/databend/raft/heap; when remove V003 raft-log store based on sled: '/var/lib/databend/raft')
        tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;

        //</_databend/meta_1/
        // ▸ df_meta/
        // ▸ heap/
        //   conf
        //   db
        //   DO_NOT_USE_THIS_DIRECTORY_FOR_ANYTHING
        //   snap.00000000269348D9

        let raft_dir = self.config.raft_dir.clone();
        let raft_dir = Path::new(&raft_dir);
        let ctx = format!(
            "when remove V003 raft-log store based on sled: '{}'",
            raft_dir.display()
        );

        let list_dir = fs::read_dir(raft_dir).context(|| format!("listing; {ctx}"))?;

        for entry in list_dir {
            let entry = entry.context(|| format!("get dir entry; {ctx}"))?;

            if entry.file_name().to_str() == Some("df_meta") {
                continue;
            }

            let p = entry.path();
            self.progress(format!("        Removing: {}", p.display()));

            if p.is_dir() {
                fs::remove_dir_all(&p)
                    .context(|| format!("remove dir {}; {ctx}", p.as_path().display()))?;
            } else {
                fs::remove_file(&p)
                    .context(|| format!("remove file {}; {ctx}", p.as_path().display()))?;
            }
        }

        Ok(())
    }
}
