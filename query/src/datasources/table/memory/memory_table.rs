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
use common_dal::InMemoryData;
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
use crate::datasources::table::memory::memory_table_stream::MemoryTableStream;
use crate::sessions::DatabendQueryContext;

pub struct MemoryTable {
    table_info: TableInfo,

    // TODO(xp): When table is dropped, remove the entry in `in_memory_data`.
    //           This requires another trait method Table::drop to customize the drop process.
    #[allow(dead_code)]
    in_memory_data: Arc<RwLock<InMemoryData<u64>>>,

    blocks: Arc<RwLock<Vec<DataBlock>>>,
}

impl MemoryTable {
    pub fn try_create(
        table_info: TableInfo,
        data_ctx: Arc<dyn DataContext<u64>>,
    ) -> Result<Box<dyn Table>> {
        let table_id = &table_info.ident.table_id;
        let in_memory_data = data_ctx.get_in_memory_data()?;

        let blocks = {
            let mut in_mem_data = in_memory_data.write();

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

        let table = Self {
            table_info,
            in_memory_data,
            blocks,
        };
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

    fn read_partitions(
        &self,
        io_ctx: Arc<TableIOContext>,
        _push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<(Statistics, Partitions)> {
        let blocks = self.blocks.read();

        let rows = blocks.iter().map(|block| block.num_rows()).sum();
        let bytes = blocks.iter().map(|block| block.memory_size()).sum();

        let statistics = Statistics::new_exact(rows, bytes);
        let parts = generate_parts(0, io_ctx.get_max_threads() as u64, blocks.len() as u64);
        Ok((statistics, parts))
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");

        let blocks = self.blocks.read();
        Ok(Box::pin(MemoryTableStream::try_create(
            ctx,
            blocks.clone(),
        )?))
    }

    async fn append_data(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        let mut s = {
            let mut inner = _insert_plan.input_stream.lock();
            (*inner).take()
        }
        .ok_or_else(|| ErrorCode::EmptyData("input stream consumed"))?;

        if _insert_plan.schema().as_ref().fields() != self.table_info.schema().as_ref().fields() {
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
        _io_ctx: Arc<TableIOContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        let mut blocks = self.blocks.write();
        blocks.clear();
        Ok(())
    }
}
