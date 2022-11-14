//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;

use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::common::FormatVersion;
use crate::meta::ColumnId;
use crate::meta::ColumnNDVs;
use crate::meta::SnapshotId;
use crate::meta::Versioned;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshotStatistics {
    /// format version of snapshot
    format_version: FormatVersion,

    /// id of snapshot
    pub snapshot_id: SnapshotId,

    pub column_ndvs: ColumnNDVs,
}

impl TableSnapshotStatistics {
    pub fn new(snapshot_id: SnapshotId, column_ndvs: ColumnNDVs) -> Self {
        Self {
            format_version: TableSnapshotStatistics::VERSION,
            snapshot_id,
            column_ndvs,
        }
    }

    pub fn format_version(&self) -> u64 {
        self.format_version
    }

    pub fn new_empty(snapshot_id: SnapshotId) -> Self {
        Self {
            format_version: TableSnapshotStatistics::VERSION,
            snapshot_id,
            column_ndvs: ColumnNDVs::default(),
        }
    }

    pub fn get_column_counts(&self) -> HashMap<ColumnId, u64> {
        self.column_ndvs.get_number_of_distinct_values()
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let data = bincode::serde::encode_to_vec(self, bincode::config::standard())?;
        Ok(data)
    }

    pub fn deserialize(data: &Vec<u8>) -> Result<Self> {
        let (t, _) =
            bincode::serde::decode_from_slice(data.as_slice(), bincode::config::standard())?;
        Ok(t)
    }
}
