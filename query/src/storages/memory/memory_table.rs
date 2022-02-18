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
use std::collections::HashSet;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use crate::sessions::QueryContext;
use crate::storages::memory::MemoryTableStream;
use crate::storages::StorageContext;
use crate::storages::StorageDescription;
use crate::storages::Table;

pub struct MemoryTable {
    table_info: TableInfo,
    blocks: Arc<RwLock<Vec<DataBlock>>>,
}

impl MemoryTable {
    pub fn try_create(ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
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

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "MEMORY".to_string(),
            comment: "MEMORY Storage Engine".to_string(),
        }
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

    fn benefit_column_prune(&self) -> bool {
        true
    }

    async fn read_partitions(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let blocks = self.blocks.read();

        let statistics = match push_downs {
            Some(push_downs) => {
                let projection_filter: Box<dyn Fn(usize) -> bool> = match push_downs.projection {
                    Some(prj) => {
                        let proj_cols = HashSet::<usize>::from_iter(prj);
                        Box::new(move |column_id: usize| proj_cols.contains(&column_id))
                    }
                    None => Box::new(|_: usize| true),
                };

                blocks
                    .iter()
                    .fold(Statistics::default(), |mut stats, block| {
                        stats.read_rows += block.num_rows() as usize;
                        stats.read_bytes += (0..block.num_columns())
                            .into_iter()
                            .collect::<Vec<usize>>()
                            .iter()
                            .filter(|cid| projection_filter(**cid))
                            .map(|cid| block.columns()[*cid].memory_size() as u64)
                            .sum::<u64>() as usize;

                        stats
                    })
            }
            None => {
                let rows = blocks.iter().map(|block| block.num_rows()).sum();
                let bytes = blocks.iter().map(|block| block.memory_size()).sum();

                Statistics::new_exact(rows, bytes, blocks.len(), blocks.len())
            }
        };

        let parts = crate::table_functions::generate_block_parts(
            0,
            ctx.get_settings().get_max_threads()? as u64,
            blocks.len() as u64,
        );
        Ok((statistics, parts))
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let push_downs = &plan.push_downs;
        let raw_blocks = self.blocks.read().clone();

        let blocks = match push_downs {
            Some(push_downs) => match &push_downs.projection {
                Some(prj) => {
                    let pruned_schema = Arc::new(self.table_info.schema().project(prj.clone()));
                    let mut pruned_blocks = Vec::with_capacity(raw_blocks.len());

                    for raw_block in raw_blocks {
                        let raw_columns = raw_block.columns();
                        let columns: Vec<ColumnRef> =
                            prj.iter().map(|idx| raw_columns[*idx].clone()).collect();

                        pruned_blocks.push(DataBlock::create(pruned_schema.clone(), columns))
                    }

                    pruned_blocks
                }
                None => raw_blocks,
            },
            None => raw_blocks,
        };

        Ok(Box::pin(MemoryTableStream::try_create(ctx, blocks)?))
    }

    async fn append_data(
        &self,
        _ctx: Arc<QueryContext>,
        stream: SendableDataBlockStream,
    ) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(stream))
    }

    async fn commit_insertion(
        &self,
        ctx: Arc<QueryContext>,
        operations: Vec<DataBlock>,
        overwrite: bool,
    ) -> Result<()> {
        let written_rows: usize = operations.iter().map(|b| b.num_rows()).sum();
        let written_bytes: usize = operations.iter().map(|b| b.memory_size()).sum();
        ctx.get_dal_context().inc_write_rows(written_rows);
        ctx.get_dal_context().inc_write_bytes(written_bytes);

        if overwrite {
            let mut blocks = self.blocks.write();
            blocks.clear();
        }
        let mut blocks = self.blocks.write();
        for block in operations {
            blocks.push(block);
        }
        Ok(())
    }

    async fn truncate(
        &self,
        _ctx: Arc<QueryContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        let mut blocks = self.blocks.write();
        blocks.clear();
        Ok(())
    }
}
