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

use common_base::BlockingWait;
use common_catalog::BlockLocation;
use common_catalog::IOContext;
use common_catalog::TableIOContext;
use common_catalog::TableSnapshot;
use common_dal::read_obj;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_meta_types::MetaId;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;

pub struct FuseTable {
    pub(crate) tbl_info: TableInfo,
}

impl FuseTable {
    pub fn try_create(tbl_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(FuseTable { tbl_info }))
    }
}

#[async_trait::async_trait]
impl Table for FuseTable {
    fn name(&self) -> &str {
        &self.tbl_info.name
    }

    fn engine(&self) -> &str {
        &self.tbl_info.engine
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.tbl_info.schema.clone())
    }

    fn get_id(&self) -> MetaId {
        self.tbl_info.table_id
    }

    fn is_local(&self) -> bool {
        false
    }

    fn read_plan(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        self.do_read_plan(io_ctx, push_downs)
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        self.do_read(io_ctx, push_downs).await
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
    pub fn table_snapshot(&self, io_ctx: Arc<TableIOContext>) -> Result<Option<TableSnapshot>> {
        let schema = &self.tbl_info.schema;
        if let Some(loc) = schema.meta().get("META_SNAPSHOT_LOCATION") {
            let da = io_ctx.get_data_accessor()?;
            let r = read_obj(da, loc.to_string()).wait_in(&io_ctx.get_runtime(), None)??;
            Ok(Some(r))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn empty_read_source_plan(&self) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            table_info: self.tbl_info.clone(),
            parts: vec![],
            statistics: Statistics::default(),
            description: "".to_string(),
            scan_plan: Default::default(),
            tbl_args: None,
            push_downs: None,
        })
    }

    pub(crate) fn to_partitions(&self, _blocks: &[BlockLocation]) -> (Statistics, Partitions) {
        todo!()
    }
}
