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

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use databend_common_catalog::plan::PartInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use paimon::DataSplit;
use paimon::DataSplitBuilder;
use paimon::DeletionFile;
use paimon::RowRange;
use paimon::spec::BinaryRow;
use paimon::spec::DataFileMeta;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SerializableDataSplit {
    pub snapshot_id: i64,
    pub partition: BinaryRow,
    pub bucket: i32,
    pub bucket_path: String,
    pub total_buckets: i32,
    pub data_files: Vec<DataFileMeta>,
    pub deletion_files: Option<Vec<Option<DeletionFile>>>,
    pub row_ranges: Option<Vec<RowRange>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PaimonPartInfo {
    pub split: SerializableDataSplit,
}

impl From<&DataSplit> for SerializableDataSplit {
    fn from(split: &DataSplit) -> Self {
        Self {
            snapshot_id: split.snapshot_id(),
            partition: split.partition().clone(),
            bucket: split.bucket(),
            bucket_path: split.bucket_path().to_string(),
            total_buckets: split.total_buckets(),
            data_files: split.data_files().to_vec(),
            deletion_files: split.data_deletion_files().map(|files| files.to_vec()),
            row_ranges: split.row_ranges().map(|ranges| ranges.to_vec()),
        }
    }
}

impl TryFrom<SerializableDataSplit> for DataSplit {
    type Error = ErrorCode;

    fn try_from(value: SerializableDataSplit) -> Result<Self> {
        let mut builder = DataSplitBuilder::new()
            .with_snapshot(value.snapshot_id)
            .with_partition(value.partition)
            .with_bucket(value.bucket)
            .with_bucket_path(value.bucket_path)
            .with_total_buckets(value.total_buckets)
            .with_data_files(value.data_files);
        if let Some(deletion_files) = value.deletion_files {
            builder = builder.with_data_deletion_files(deletion_files);
        }
        if let Some(row_ranges) = value.row_ranges {
            builder = builder.with_row_ranges(row_ranges);
        }
        builder
            .build()
            .map_err(|err| ErrorCode::ReadTableDataError(format!("invalid paimon split: {err:?}")))
    }
}

#[typetag::serde(name = "paimon")]
impl PartInfo for PaimonPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, other: &Box<dyn PartInfo>) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.split.snapshot_id.hash(&mut hasher);
        self.split.bucket.hash(&mut hasher);
        self.split.bucket_path.hash(&mut hasher);
        hasher.finish()
    }
}
