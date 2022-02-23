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

use common_exception::Result;
use common_planners::DeletePlan;
use common_planners::Expression;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::pruning::BlockPruner;
use crate::storages::fuse::FuseTable;
use crate::storages::index::BlockStatistics;
use crate::storages::index::RangeFilter;

type Pred = Box<dyn Fn(&BlockStatistics) -> Result<bool> + Send + Sync + Unpin>;
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
                            segments_kept.push(segment_info);
                        } else {
                            let new_segment = self.delete_rows(blocks, &expr).await?;
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

    async fn delete_rows(
        &self,
        _blk_metas: Vec<BlockMeta>,
        _expr: &Expression,
    ) -> Result<SegmentInfo> {
        todo!()
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
