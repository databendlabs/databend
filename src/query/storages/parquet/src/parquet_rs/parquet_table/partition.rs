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

use std::sync::Arc;

use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;

use super::ParquetTable;
use crate::parquet_rs::pruning::ParquetPartitionPruner;

impl ParquetTable {
    #[inline]
    #[async_backtrace::framed]
    pub(super) async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let pruner = ParquetPartitionPruner::try_create(
            ctx.clone(),
            &self.arrow_schema,
            self.schema_descr.clone(),
            &self.schema_from,
            &push_down,
            self.read_options,
            self.compression_ratio,
            false,
        )?;

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

        let (stats, partitions) = pruner
            .read_and_prune_partitions(self.operator.clone(), &file_locations)
            .await?;

        let partition_kind = PartitionsShuffleKind::Mod;
        let partitions = partitions
            .into_iter()
            .map(|p| p.convert_to_part_info())
            .collect();
        Ok((stats, Partitions::create_nolazy(partition_kind, partitions)))
    }
}
