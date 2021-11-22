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
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;

use crate::catalogs::Table;
use crate::datasources::common::generate_parts;
use crate::datasources::context::DataSourceContext;
use crate::datasources::table::memory::memory_table_stream::MemoryTableStream;
use crate::sessions::DatabendQueryContextRef;

pub struct MemoryTable {
    table_info: TableInfo,
    blocks: Arc<RwLock<Vec<DataBlock>>>,
}

impl MemoryTable {
    pub fn try_create(table_info: TableInfo, ctx: DataSourceContext) -> Result<Box<dyn Table>> {
        let table_id = &table_info.ident.table_id;
        let blocks = {
            let mut in_mem_data = ctx.in_memory_data.write();
            let x = in_mem_data.get(table_id);
            match x {
                None => {
                    let blocks = Arc::new(RwLock::new(vec![]));
                    in_mem_data.insert(*table_id, blocks.clone());
                    blocks
                }
                Some(blocks) => blocks.clone(),
            }
        };

        let table = Self { table_info, blocks };
        Ok(Box::new(table))
    }
}

#[async_trait::async_trait]
impl Table for MemoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        ctx: DatabendQueryContextRef,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let blocks = self.blocks.read();

        let rows = blocks.iter().map(|block| block.num_rows()).sum();
        let bytes = blocks.iter().map(|block| block.memory_size()).sum();

        let statistics = Statistics::new_exact(rows, bytes);
        let parts = generate_parts(
            0,
            ctx.get_settings().get_max_threads()? as u64,
            blocks.len() as u64,
        );
        Ok((statistics, parts))
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _plan: &ReadDataSourcePlan,
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
        insert_plan: InsertIntoPlan,
        mut stream: SendableDataBlockStream,
    ) -> Result<()> {
        if insert_plan.schema().as_ref().fields() != self.table_info.schema().as_ref().fields() {
            return Err(ErrorCode::BadArguments("DataBlock schema mismatch"));
        }

        while let Some(block) = stream.next().await {
            let block = block?;
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
