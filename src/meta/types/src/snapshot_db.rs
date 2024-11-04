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

use std::fs;
use std::io;
use std::io::BufReader;
use std::sync::Arc;

use futures_util::stream::BoxStream;
use openraft::SnapshotId;
use rotbl::v001::stat::RotblStat;
use rotbl::v001::Rotbl;
use rotbl::v001::SeqMarked;

use crate::sys_data::SysData;
use crate::SnapshotMeta;

/// A readonly leveled map that owns the data.
#[derive(Debug, Clone)]
pub struct DB {
    pub path: String,
    pub meta: SnapshotMeta,
    pub sys_data: SysData,
    pub rotbl: Arc<Rotbl>,
}

impl AsRef<SysData> for DB {
    fn as_ref(&self) -> &SysData {
        &self.sys_data
    }
}

impl DB {
    pub fn new(
        path: impl ToString,
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
            path: path.to_string(),
            meta: snapshot_meta,
            sys_data,
            rotbl: r,
        };
        Ok(s)
    }

    /// Create an `BufReader<std::fs::File>` pointing to the same file of this db
    pub fn open_file(&self) -> Result<BufReader<fs::File>, io::Error> {
        let f = fs::OpenOptions::new()
            .create(false)
            .create_new(false)
            .read(true)
            .open(self.path())?;

        let buf_f = BufReader::with_capacity(16 * 1024 * 1024, f);
        Ok(buf_f)
    }

    pub fn inner_range(&self) -> BoxStream<'static, Result<(String, SeqMarked), io::Error>> {
        self.rotbl.range(..)
    }

    pub fn inner(&self) -> &Arc<Rotbl> {
        &self.rotbl
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn snapshot_meta(&self) -> &SnapshotMeta {
        &self.meta
    }

    pub fn file_size(&self) -> u64 {
        self.rotbl.file_size()
    }

    pub fn stat(&self) -> &RotblStat {
        self.rotbl.stat()
    }

    pub fn sys_data(&self) -> &SysData {
        &self.sys_data
    }
}
