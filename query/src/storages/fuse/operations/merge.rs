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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::Result;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use common_planners::Statistics;

use super::AppendOperationLogEntry;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::DEFAULT_BLOCK_PER_SEGMENT;
use crate::storages::fuse::DEFAULT_ROW_PER_BLOCK;
use crate::storages::fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::storages::fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use crate::storages::storage_table::Table;
use crate::storages::storage_table_read_plan::get_description;

pub struct MergeWriter {
    plan: ReadDataSourcePlan,
    remain_blocks: Vec<BlockMeta>,
}

// prepare: merge the blocks write the segments.
// exec: write block
// genera the segments
// finish

impl FuseTable {
    pub async fn select_blocks_to_merge(
        &self,
        ctx: &Arc<QueryContext>,
    ) -> Result<(Statistics, Partitions)> {
        let snapshot = self.read_table_snapshot(ctx.as_ref()).await?;
        let max_row_per_block =
            self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_ROW_PER_BLOCK) as u64;
        let min_rows_per_block = (max_row_per_block as f64 * 0.8) as u64;
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        match snapshot {
            Some(snapshot) => {
                let mut block_metas = Vec::new();
                let mut remain_blocks = Vec::new();
                let reader = MetaReaders::segment_info_reader(ctx.as_ref());
                for (x, ver) in &snapshot.segments {
                    let mut need_merge = false;
                    let mut remains = Vec::new();
                    let res = reader.read(x, None, *ver).await?;
                    res.blocks.iter().for_each(|b| {
                        if b.row_count > max_row_per_block || b.row_count < min_rows_per_block {
                            block_metas.push(b.clone());
                            need_merge = true;
                        } else {
                            remains.push(b.clone());
                        }
                    });

                    if !need_merge && res.blocks.len() == block_per_seg {
                        let log_entry = AppendOperationLogEntry::new(x.to_string(), res);
                        ctx.push_precommit_block(DataBlock::try_from(log_entry)?);
                        continue;
                    }

                    remain_blocks.append(&mut remains);
                }

                let partitions_scanned = block_metas.len();
                if partitions_scanned == 0 {
                    return Ok((Statistics::default(), vec![]));
                }
                let partitions_total = snapshot.summary.block_count as usize;

                let (mut statistics, parts) = Self::to_partitions(&block_metas, None);
                // Update planner statistics.
                statistics.partitions_total = partitions_total;
                statistics.partitions_scanned = partitions_scanned;

                // Update context statistics.
                ctx.get_dal_context()
                    .get_metrics()
                    .inc_partitions_total(partitions_total as u64);
                ctx.get_dal_context()
                    .get_metrics()
                    .inc_partitions_scanned(partitions_scanned as u64);

                Ok((statistics, parts))
            }
            None => Ok((Statistics::default(), vec![])),
        }
    }

    async fn build_read_datasource_plan(
        &self,
        ctx: &Arc<QueryContext>,
        catalog: String,
    ) -> Result<ReadDataSourcePlan> {
        let (statistics, parts) = self.select_blocks_to_merge(ctx).await?;
        let table_info = self.get_table_info();
        let description = get_description(table_info, &statistics);

        Ok(ReadDataSourcePlan {
            catalog,
            source_info: SourceInfo::TableSource(table_info.clone()),
            scan_fields: None,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs: None,
        })
    }

    async fn merge(&self, ctx: &Arc<QueryContext>, plan: &ReadDataSourcePlan) -> Result<()> {
        let mut pipeline = NewPipeline::create();
        let read_source_plan = plan.clone();
        ctx.try_set_partitions(plan.parts.clone())?;
        let res = self.do_read2(ctx.clone(), &read_source_plan, &mut pipeline);
        if let Err(e) = res {
            return Err(e);
        }

        todo!()
    }
}

impl MergeWriter {
    async fn prepare(&mut self) {}
}
