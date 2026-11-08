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

//! Snapshot-level segment zone map.
//!
//! One parquet file per zone map build holds, for every covered segment, its path,
//! block count and the `min`, `max` and `null_count` of every column that has
//! statistics, copied from the segment summaries. Queries read the few columns
//! their filter references and skip segments before reading any segment info.
//!
//! The file is laid out in row groups that builds and queries stream one at a
//! time, so neither holds the whole zone map in memory; the chunk statistics in
//! the footer let a query skip row groups its filter cannot match. See [`format`].
//!
//! Recluster refreshes the zone map after each committed round; every other
//! commit carries the pointer along, so the zone map may lag behind the
//! snapshot. It is only used to exclude segments, which keeps a lagging zone
//! map correct. See [`build`] for when and what is refreshed.

mod build;
mod format;
mod prune;

pub use build::RefreshOutcome;
pub use prune::prune_segments_by_zone_map;
