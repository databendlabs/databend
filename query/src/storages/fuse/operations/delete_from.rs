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
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::DeletePlan;
use common_planners::Expression;

use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockReader;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::pruning::BlockPruner;
use crate::storages::fuse::pruning::Pred;
use crate::storages::fuse::FuseTable;
use crate::storages::index::BlockStatistics;
use crate::storages::index::RangeFilter;

impl FuseTable {
    pub async fn do_delete_from(&self, ctx: Arc<QueryContext>, plan: DeletePlan) -> Result<()> {
        if let Some(expr) = plan.selection {
            let mut segments_kept: Vec<Arc<SegmentInfo>> = vec![];
            let mut segments_new: Vec<SegmentInfo> = vec![];

            if let Some(snapshot) = self.read_table_snapshot(ctx.as_ref()).await? {
                let segment_locs = snapshot.segments.clone();

                if segment_locs.is_empty() {
                    return Ok(());
                } else {
                    let schema = self.table_info.schema();
                    let block_pred: Pred = {
                        let verifiable_expression = RangeFilter::try_create(&expr, schema)?;
                        Box::new(move |v: &BlockStatistics| verifiable_expression.eval(v))
                    };

                    for loc in segment_locs {
                        let reader = MetaReaders::segment_info_reader(&ctx);
                        let segment_info = reader.read(loc).await?;
                        let blocks =
                            BlockPruner::filter_segment(segment_info.as_ref(), &block_pred)?;
                        if blocks.is_empty() {
                            // the segment and the all the blocks should be kept definitely
                            segments_kept.push(segment_info);
                        } else {
                            // Maybe there are rows should be deleted (false-positive of pruner)
                            // use `delete_rows` to do the real data filtering.
                            let new_segment = self.delete_rows(ctx.clone(), blocks, &expr).await?;
                            segments_new.push(new_segment);
                        }
                    }

                    let new_snapshot = self.merge_segments(&segments_kept, &segments_new);
                    self.commit_deletion(new_snapshot).await?
                }
            }
        } else {
            // TODO shortcut for empty selection (un-conditional deletion) and ...?
        }
        Ok(())
    }

    // TODO rename this method
    async fn delete_rows(
        &self,
        ctx: Arc<QueryContext>,
        blk_metas: Vec<BlockMeta>,
        expr: &Expression,
    ) -> Result<SegmentInfo> {
        let operator = ctx.get_storage_operator().await?;
        let schema = self.table_info.schema();
        let predicate_executor = Self::expr_executor(&schema, expr)?;
        predicate_executor.validate()?;
        let mut new_blocks = vec![];
        for blk_meta in blk_metas {
            let loc = &blk_meta.location;
            let size = blk_meta.file_size;
            let projection = vec![];
            let meta_reader = MetaReaders::block_meta_reader(ctx.clone());
            let mut block_reader = BlockReader::new(
                operator.clone(),
                loc.path.clone(),
                self.table_info.schema(),
                projection,
                size,
                meta_reader,
            );

            let block = block_reader.read().await.map_err(|e| {
                ErrorCode::ParquetError(format!("fail to read block {}, {}", loc.path.as_str(), e))
            })?;

            let filter_block = predicate_executor.execute(&block)?;
            let filter_block = Self::inverse(filter_block);

            // TODO optimize this
            // use arrow's filter?
            let res = DataBlock::filter_block(&block, filter_block.column(0))?;
            if res.num_rows() != block.num_rows() {
                // nothing filtered, false positive
                new_blocks.push(blk_meta)
            }
        }
        todo!()
    }

    fn inverse(blcok: DataBlock) -> DataBlock {
        todo!()
    }

    // duplicated code
    fn expr_executor(schema: &DataSchemaRef, expr: &Expression) -> Result<ExpressionExecutor> {
        let expr_field = expr.to_data_field(schema)?;
        let expr_schema = DataSchemaRefExt::create(vec![expr_field]);
        ExpressionExecutor::try_create(
            "filter expression executor",
            schema.clone(),
            expr_schema,
            vec![expr.clone()],
            false,
        )
    }

    fn merge_segments(
        &self,
        _kept: &[Arc<SegmentInfo>],
        _new_segments: &[SegmentInfo],
    ) -> TableSnapshot {
        todo!()
    }

    async fn commit_deletion(&self, _new_snapshot: TableSnapshot) -> Result<()> {
        todo!()
    }
}
