//  Copyright 2021 Datafuse Labs.
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
//

use std::any::Any;
use std::sync::Arc;

use common_context::DataContext;
use common_context::IOContext;
use common_context::TableIOContext;
use common_dal::read_obj;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use super::util;
use crate::catalogs::Table;
use crate::datasources::table::fuse::BlockMeta;
use crate::datasources::table::fuse::TableSnapshot;

pub struct FuseTable {
    pub(crate) table_info: TableInfo,
}

impl FuseTable {
    pub fn try_create(
        table_info: TableInfo,
        _data_ctx: Arc<dyn DataContext<u64>>,
    ) -> Result<Box<dyn Table>> {
        Ok(Box::new(FuseTable { table_info }))
    }
}

#[async_trait::async_trait]
impl Table for FuseTable {
    fn is_local(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn read_partitions(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<(Statistics, Partitions)> {
        self.do_read_partitions(io_ctx.as_ref(), push_downs)
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        self.do_read(io_ctx, &plan.push_downs).await
    }

    async fn append_data(
        &self,
        io_ctx: Arc<TableIOContext>,
        insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        self.do_append(io_ctx, insert_plan).await
    }

    async fn truncate(
        &self,
        io_ctx: Arc<TableIOContext>,
        truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        self.do_truncate(io_ctx, truncate_plan).await
    }
}

impl FuseTable {
    pub(crate) fn snapshot_loc(&self) -> Option<String> {
        self.table_info
            .options()
            .get(util::TBL_OPT_KEY_SNAPSHOT_LOC)
            .cloned()
    }

    pub(crate) async fn table_snapshot(
        &self,
        io_ctx: &TableIOContext,
    ) -> Result<Option<TableSnapshot>> {
        if let Some(loc) = self.snapshot_loc() {
            let da = io_ctx.get_data_accessor()?;
            Ok(Some(read_obj(da, loc.to_string()).await?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn to_partitions(&self, blocks_metas: &[BlockMeta]) -> (Statistics, Partitions) {
        blocks_metas.iter().fold(
            (Statistics::default(), Partitions::default()),
            |(mut stats, mut parts), item| {
                stats.read_rows += item.row_count as usize;
                stats.read_bytes += item.block_size as usize;
                parts.push(Part {
                    name: item.location.location.clone(),
                    version: 0,
                });
                (stats, parts)
            },
        )
    }
}
