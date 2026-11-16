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

use databend_common_frozen_api::FrozenAPI;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::Location;

/// Pointer to a snapshot-level segment zone map file.
///
/// The file is a single row group parquet file with one row per covered
/// segment: the segment path, its block count, and the `min`, `max` and
/// `null_count` of every column that has statistics, copied from the segment
/// summary. Rows are keyed by segment path, and a segment's statistics never
/// change once written, so a zone map built from any snapshot is safe to use with
/// any other snapshot: segments missing from the zone map are kept, and rows of
/// segments that are no longer live are ignored.
///
/// Only the footer position is recorded here. Readers fetch the footer (one
/// open-ended range read to the end of the file) and then the column chunks they
/// need, which keeps the snapshot small no matter how many columns the table
/// has. Everything else about the file, including its row count and column
/// list, is in the footer.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct SegmentZoneMapMeta {
    pub location: Location,
    /// Offset of the parquet footer metadata; it runs to the end of the file.
    pub footer_offset: u64,
}

impl SegmentZoneMapMeta {
    pub const VERSION: u64 = 1;
}
