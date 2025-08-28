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

use std::fmt;
use std::fs;
use std::io;
use std::io::BufReader;
use std::sync::Arc;

use futures_util::stream::BoxStream;
use log::info;
use openraft::SnapshotId;
use rotbl::v001::stat::RotblStat;
use rotbl::v001::Rotbl;
use rotbl::v001::SeqMarked;

use crate::raft_types::SnapshotMeta;
use crate::sys_data::SysData;

/// A readonly leveled map that owns the data.
#[derive(Clone)]
pub struct DB {
    pub storage_path: String,
    pub rel_path: String,
    pub meta: SnapshotMeta,
    pub sys_data: SysData,
    pub rotbl: Arc<Rotbl>,
}

impl fmt::Debug for DB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DB")
            .field("storage_path", &self.storage_path)
            .field("rel_path", &self.rel_path)
            .field("meta", &self.meta)
            .field("sys_data", &self.sys_data)
            .finish()
    }
}

impl AsRef<SysData> for DB {
    fn as_ref(&self) -> &SysData {
        &self.sys_data
    }
}

impl DB {
    pub fn new(
        storage_path: impl ToString,
        rel_path: impl ToString,
        snapshot_id: SnapshotId,
        r: Arc<Rotbl>,
    ) -> Result<Self, io::Error> {
        let sys_data = r.meta().user_data();
        let sys_data: SysData = serde_json::from_str(sys_data).unwrap();

        let snapshot_meta = SnapshotMeta {
            last_log_id: *sys_data.last_applied_ref(),
            last_membership: sys_data.last_membership_ref().clone(),
            snapshot_id,
        };

        let s = Self {
            storage_path: storage_path.to_string(),
            rel_path: rel_path.to_string(),
            meta: snapshot_meta,
            sys_data,
            rotbl: r,
        };
        Ok(s)
    }

    /// Create an `BufReader<std::fs::File>` pointing to the same file of this db
    pub fn open_file(&self) -> Result<BufReader<fs::File>, io::Error> {
        info!("Opening file for DB at path: {}", self.path());
        let f = fs::OpenOptions::new()
            .create(false)
            .create_new(false)
            .read(true)
            .open(self.path())
            .map_err(|e| {
                io::Error::new(e.kind(), format!("{}; when:(open: {})", e, self.path()))
            })?;

        let buf_f = BufReader::with_capacity(16 * 1024 * 1024, f);
        Ok(buf_f)
    }

    pub fn inner_range(&self) -> BoxStream<'static, Result<(String, SeqMarked), io::Error>> {
        self.rotbl.range(..)
    }

    pub fn inner(&self) -> &Arc<Rotbl> {
        &self.rotbl
    }

    pub fn path(&self) -> String {
        format!("{}/{}", self.storage_path, self.rel_path)
    }

    pub fn snapshot_meta(&self) -> &SnapshotMeta {
        &self.meta
    }

    pub fn file_size(&self) -> u64 {
        self.rotbl.file_size()
    }

    /// Get the statistics of the snapshot database.
    pub fn db_stat(&self) -> DBStat {
        let stat = self.stat();
        let access_stat = self.rotbl.access_stat();

        let divider_block_num = std::cmp::max(stat.block_num as u64, 1);

        DBStat {
            block_num: stat.block_num as u64,
            key_num: stat.key_num,
            data_size: stat.data_size,
            index_size: stat.index_size,
            avg_block_size: stat.data_size / divider_block_num,
            avg_keys_per_block: stat.key_num / divider_block_num,
            read_block: access_stat.read_block(),
            read_block_from_cache: access_stat.read_block_from_cache(),
            read_block_from_disk: access_stat.read_block_from_disk(),
        }
    }

    pub fn stat(&self) -> &RotblStat {
        self.rotbl.stat()
    }

    pub fn sys_data(&self) -> &SysData {
        &self.sys_data
    }

    pub fn last_seq(&self) -> u64 {
        self.sys_data.curr_seq()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DBStat {
    /// Total number of blocks.
    pub block_num: u64,

    /// Total number of keys.
    pub key_num: u64,

    /// Size of all user data(in blocks) in bytes.
    pub data_size: u64,

    /// Size of serialized block index in bytes.
    pub index_size: u64,

    /// Average size in bytes of a block.
    pub avg_block_size: u64,

    /// Average number of keys per block.
    pub avg_keys_per_block: u64,

    /// Total number of read block from cache or from disk.
    pub read_block: u64,

    /// Total number of read block from cache.
    pub read_block_from_cache: u64,

    /// Total number of read block from disk.
    pub read_block_from_disk: u64,
}

#[cfg(test)]
mod tests {

    use rotbl::storage::impls::fs::FsStorage;
    use rotbl::v001::Config;
    use rotbl::v001::RotblMeta;

    use super::*;

    /// Debug should not output db cache data.
    #[test]
    fn test_db_debug() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(tmp_dir.path().to_path_buf());
        let config = Config::default();
        let path = "test_rotbl";
        let rotbl =
            Rotbl::create_table(storage, config, path, RotblMeta::new(1, "foo"), []).unwrap();

        let db = DB {
            storage_path: "a".to_string(),
            rel_path: "b".to_string(),
            meta: Default::default(),
            sys_data: Default::default(),
            rotbl: Arc::new(rotbl),
        };

        assert_eq!(
            format!("{:?}", db),
            r#"DB { storage_path: "a", rel_path: "b", meta: SnapshotMeta { last_log_id: None, last_membership: StoredMembership { log_id: None, membership: Membership { configs: [], nodes: {} } }, snapshot_id: "" }, sys_data: SysData { last_applied: None, last_membership: StoredMembership { log_id: None, membership: Membership { configs: [], nodes: {} } }, nodes: {}, sequence: 0, key_counts: {}, sm_features: {} } }"#
        );
    }
}
