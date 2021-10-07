// Copyright 2020 Datafuse Labs.
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

//! `catalog` defines catalog related data types, such as table or database.

mod table_io_context;
mod table_snapshot;

#[cfg(test)]
mod table_io_context_test;

pub use table_io_context::IOContext;
pub use table_io_context::TableIOContext;
pub use table_snapshot::BlockLocation;
pub use table_snapshot::BlockMeta;
pub use table_snapshot::ColStats;
pub use table_snapshot::ColumnId;
pub use table_snapshot::Location;
pub use table_snapshot::RawBlockStats;
pub use table_snapshot::SegmentInfo;
pub use table_snapshot::SnapshotId;
pub use table_snapshot::Stats;
pub use table_snapshot::TableSnapshot;
