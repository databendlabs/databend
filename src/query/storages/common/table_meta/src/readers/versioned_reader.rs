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

use std::io::Read;

use databend_common_exception::Result;

use crate::meta::load_json;
use crate::meta::TableSnapshotStatistics;
use crate::meta::TableSnapshotStatisticsVersion;

pub trait VersionedReader<T> {
    type TargetType;

    fn read<R>(&self, read: R) -> Result<Self::TargetType>
    where R: Read + Unpin + Send;
}

impl VersionedReader<TableSnapshotStatistics> for TableSnapshotStatisticsVersion {
    type TargetType = TableSnapshotStatistics;

    fn read<R>(&self, reader: R) -> Result<TableSnapshotStatistics>
    where R: Read + Unpin + Send {
        let r = match self {
            TableSnapshotStatisticsVersion::V0(v) => {
                let ts = load_json(reader, v)?;
                TableSnapshotStatistics::from(ts)
            }
            TableSnapshotStatisticsVersion::V2(v) => load_json(reader, v)?,
        };
        Ok(r)
    }
}
