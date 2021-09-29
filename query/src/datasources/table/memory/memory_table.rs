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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Extras;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;

use crate::catalogs::Table;
use crate::catalogs::TableInfo;
use crate::datasources::common::generate_parts;
use crate::datasources::table::memory::memory_table_stream::MemoryTableStream;
use crate::sessions::DatabendQueryContextRef;

pub struct MemoryTable {
    tbl_info: TableInfo,
    blocks: Arc<RwLock<Vec<DataBlock>>>,
}

impl MemoryTable {
    pub fn try_create(tbl_info: TableInfo) -> Result<Box<dyn Table>> {
        let table = Self {
            tbl_info,
            blocks: Arc::new(RwLock::new(vec![])),
        };
        Ok(Box::new(table))
    }
}

#[async_trait::async_trait]
impl Table for MemoryTable {
    fn name(&self) -> &str {
        &self.tbl_info.name
    }

    fn database(&self) -> &str {
        &self.tbl_info.db
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

    fn get_id(&self) -> u64 {
        self.tbl_info.table_id
    }

    fn is_local(&self) -> bool {
        true
    }

    fn is_stateful(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        ctx: DatabendQueryContextRef,
        push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        let blocks = self.blocks.read();
        let rows = blocks.iter().map(|block| block.num_rows()).sum();
        let bytes = blocks.iter().map(|block| block.memory_size()).sum();

        let tbl_info = &self.tbl_info;
        let db = &tbl_info.db;
        Ok(ReadDataSourcePlan {
            db: db.to_string(),
            table: self.name().to_string(),
            table_id: tbl_info.table_id,
            table_version: None,
            schema: tbl_info.schema.clone(),
            parts: generate_parts(
                0,
                ctx.get_settings().get_max_threads()?,
                blocks.len() as u64,
            ),
            statistics: Statistics::new_exact(rows, bytes),
            description: format!("(Read from Memory Engine table  {}.{})", db, self.name()),
            scan_plan: Default::default(),
            remote: false,
            tbl_args: None,
            push_downs,
        })
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let blocks = self.blocks.read();
        Ok(Box::pin(MemoryTableStream::try_create(
            ctx,
            blocks.clone(),
        )?))
    }

    async fn append_data(
        &self,
        _ctx: DatabendQueryContextRef,
        insert_plan: common_planners::InsertIntoPlan,
    ) -> Result<()> {
        let mut s = {
            let mut inner = insert_plan.input_stream.lock();
            (*inner).take()
        }
        .ok_or_else(|| ErrorCode::EmptyData("input stream consumed"))?;

        if insert_plan.schema().as_ref().fields() != self.tbl_info.schema.as_ref().fields() {
            return Err(ErrorCode::BadArguments("DataBlock schema mismatch"));
        }

        while let Some(block) = s.next().await {
            let mut blocks = self.blocks.write();
            blocks.push(block);
        }
        Ok(())
    }

    async fn truncate(
        &self,
        _ctx: DatabendQueryContextRef,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        let mut blocks = self.blocks.write();
        blocks.clear();
        Ok(())
    }
}
