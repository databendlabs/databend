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

use std::any::Any;
use std::sync::Arc;

use common_context::TableIOContext;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::MetaId;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

#[async_trait::async_trait]
pub trait Table: Sync + Send {
    fn name(&self) -> &str;
    fn engine(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> Result<DataSchemaRef>;
    fn get_id(&self) -> MetaId;
    fn is_local(&self) -> bool;

    // Some tables may have internal states, like MemoryTable
    // their instances will be kept, instead of dropped after used
    fn is_stateful(&self) -> bool {
        false
    }

    // Get the read source plan.
    fn read_plan(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: Option<Extras>,
        partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan>;

    // Read block data from the underling.
    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        _push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream>;

    // temporary added, pls feel free to rm it
    async fn append_data(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "append data for local table {} is not implemented",
            self.name()
        )))
    }

    async fn truncate(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "truncate for local table {} is not implemented",
            self.name()
        )))
    }
}

pub type TablePtr = Arc<dyn Table>;

pub trait ToTableInfo {
    /// Collect information through Table trait methods and build a TableInfo
    fn to_table_info(&self, db: &str) -> Result<TableInfo>;
}

impl<T: Table> ToTableInfo for T {
    fn to_table_info(&self, db: &str) -> Result<TableInfo> {
        let ti = TableInfo {
            // TODO not supported yet. maybe removed
            database_id: 0,
            table_id: self.get_id(),
            // TODO not supported yet.
            version: 0,
            db: db.to_string(),
            name: self.name().to_string(),
            is_local: self.is_local(),
            schema: self.schema()?,
            engine: self.engine().to_string(),
            options: Default::default(),
        };

        Ok(ti)
    }
}
