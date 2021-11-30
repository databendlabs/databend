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
use std::convert::TryFrom;
use std::sync::Arc;

use common_dal::read_obj;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::operations::AppendOperationLogEntry;
use crate::storages::StorageContext;
use crate::storages::Table;

pub struct FuseTable {
    pub(crate) table_info: TableInfo,
}

impl FuseTable {
    pub fn try_create(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
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

    fn benefit_column_prune(&self) -> bool {
        true
    }

    async fn read_partitions(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs).await
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        self.do_read(ctx, &plan.push_downs).await
    }

    async fn append_data(
        &self,
        ctx: Arc<QueryContext>,
        stream: SendableDataBlockStream,
    ) -> Result<SendableDataBlockStream> {
        let log = self.append_trunks(ctx, stream).await?;
        let blocks = match log {
            Some(op_log) => vec![DataBlock::try_from(op_log)?],
            _ => vec![],
        };
        Ok(Box::pin(DataBlockStream::create(
            AppendOperationLogEntry::schema(),
            None,
            blocks,
        )))
    }

    async fn commit(
        &self,
        _ctx: Arc<QueryContext>,
        operations: Vec<DataBlock>,
        overwrite: bool,
    ) -> Result<()> {
        // only append operation supported currently
        let append_log_entries = operations
            .iter()
            .map(AppendOperationLogEntry::try_from)
            .collect::<Result<Vec<AppendOperationLogEntry>>>()?;
        self.do_commit(_ctx, append_log_entries, overwrite).await
    }

    async fn truncate(
        &self,
        ctx: Arc<QueryContext>,
        truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        self.do_truncate(ctx, truncate_plan).await
    }
}

impl FuseTable {
    pub(crate) fn snapshot_loc(&self) -> Option<String> {
        self.table_info
            .options()
            .get(TBL_OPT_KEY_SNAPSHOT_LOC)
            .cloned()
    }

    pub(crate) async fn table_snapshot(
        &self,
        ctx: Arc<QueryContext>,
    ) -> Result<Option<TableSnapshot>> {
        if let Some(loc) = self.snapshot_loc() {
            let da = ctx.get_data_accessor()?;
            Ok(Some(read_obj(da, loc.to_string()).await?))
        } else {
            Ok(None)
        }
    }
}
