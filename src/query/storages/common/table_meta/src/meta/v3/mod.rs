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

// we use "frozen" types of table meta, to make sure that the type we used for
// bincode deserialization is compatible with the type we used for bincode serialization.
mod frozen;
mod segment;
mod snapshot;
mod table_snapshot_statistics;

pub use segment::SegmentInfo;
pub use snapshot::TableSnapshot;
pub use table_snapshot_statistics::TableSnapshotStatistics;
