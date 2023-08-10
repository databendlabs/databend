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
use std::sync::Arc;

use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;

use super::table::ParquetRSTable;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct ParquetRSPart {
    pub location: String,
}

#[typetag::serde(name = "parquet_rs_part")]
impl PartInfo for ParquetRSPart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<ParquetRSPart>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.location.hash(&mut s);
        s.finish()
    }
}

impl ParquetRSPart {
    pub fn from_part(info: &PartInfoPtr) -> Result<&ParquetRSPart> {
        info.as_any()
            .downcast_ref::<ParquetRSPart>()
            .ok_or(ErrorCode::Internal(
                "Cannot downcast from PartInfo to ParquetRSPart.",
            ))
    }
}

impl ParquetRSTable {
    #[inline]
    #[async_backtrace::framed]
    pub(super) async fn do_read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let file_locations = match &self.files_to_read {
            Some(files) => files
                .iter()
                .map(|f| (f.path.clone(), f.size))
                .collect::<Vec<_>>(),
            None => if self.operator.info().can_blocking() {
                self.files_info.blocking_list(&self.operator, false, None)
            } else {
                self.files_info.list(&self.operator, false, None).await
            }?
            .into_iter()
            .map(|f| (f.path, f.size))
            .collect::<Vec<_>>(),
        };

        // TODO:
        // The second filed of `file_locations` is size of the file.
        // It will be used for judging if we need to read small parquet files at once to reduce IO.
        let partitions = file_locations
            .into_iter()
            .map(
                |(location, _)| Arc::new(Box::new(ParquetRSPart { location }) as Box<dyn PartInfo>),
            )
            .collect();

        // TODOs:
        // - collect exact statistics.
        // - use stats to prune row groups.
        // - make one row group one partition.

        // We cannot get the exact statistics of partitions from parquet files.
        // It's because we will not read metadata of parquet files for parquet_rs.
        // Metadata will be read before reading data.
        Ok((
            PartStatistics::default(),
            Partitions::create_nolazy(PartitionsShuffleKind::Mod, partitions),
        ))
    }
}
