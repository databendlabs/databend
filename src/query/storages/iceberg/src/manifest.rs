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

use std::collections::HashMap;

use serde::Deserialize;

/// item in manifest list file
/// read manifest file by this struct
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ManifestPtr {
    pub manifest_path: String,
    pub manifest_length: usize,
    pub partition_spec_id: usize,
    pub added_snapshot_id: usize,
    pub existing_data_files_count: usize,
    pub deleted_data_files_count: usize,
    pub partitions: Vec<ManiPart>,
    pub added_rows_count: usize,
    pub existing_rows_count: usize,
    pub deleted_rows_count: usize,
}

/// item of manifest spec in `ManifestPtr`
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ManiPart {
    pub contains_null: bool,
    pub lower_bound: String,
    pub upper_bound: String,
}

/// manifest file
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Manifest {
    pub status: isize,
    pub snapshot_id: i64,
    pub data_file: DataFile,
}

/// data file
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct DataFile {
    pub file_path: String,
    pub file_format: String,
    pub partition: HashMap<String, u64>,
    pub record_count: usize,
    pub file_size_in_bytes: usize,
    pub block_size_in_bytes: usize,
    pub column_sizes: HashMap<u64, u64>,
    pub value_counts: HashMap<u64, u64>,
    pub null_value_counts: HashMap<u64, u64>,
    pub lower_bounds: HashMap<u64, String>,
    pub upper_bounds: HashMap<u64, String>,
    pub key_metadata: Option<String>,
    pub split_offsets: Vec<u64>,
}
