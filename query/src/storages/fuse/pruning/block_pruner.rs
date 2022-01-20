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

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Extras;
use common_tracing::tracing;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::snapshot_location;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::index::BlockStatistics;
use crate::storages::index::RangeFilter;

pub struct BlockPruner {
    table_snapshot_location: String,
}

type Pred = Box<dyn Fn(&BlockStatistics) -> Result<bool> + Send + Sync + Unpin>;
impl BlockPruner {
    pub fn new(table_snapshot: &TableSnapshot) -> Self {
        Self {
            table_snapshot_location: snapshot_location(&table_snapshot.snapshot_id),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(ctx.id = ctx.get_id().as_str()))]
    pub async fn apply(
        &self,
        schema: DataSchemaRef,
        push_down: &Option<Extras>,
        ctx: &QueryContext,
    ) -> Result<Vec<BlockMeta>> {
        let block_pred: Pred = match push_down {
            Some(exprs) if !exprs.filters.is_empty() => {
                // for the time being, we only handle the first expr
                let verifiable_expression = RangeFilter::try_create(&exprs.filters[0], schema)?;
                Box::new(move |v: &BlockStatistics| verifiable_expression.eval(v))
            }
            _ => Box::new(|_: &BlockStatistics| Ok(true)),
        };

        let reader = MetaReaders::table_snapshot_reader(ctx);
        let snapshot = reader.read(self.table_snapshot_location.as_str()).await?;
        let segment_num = snapshot.segments.len();
        let segment_locs = snapshot.segments.clone();

        if segment_locs.is_empty() {
            return Ok(vec![]);
        };

        let res = futures::stream::iter(segment_locs)
            .map(|seg_loc| async {
                let reader = MetaReaders::segment_info_reader(ctx);
                let segment_info = reader.read(seg_loc).await?;
                Self::filter_segment(segment_info.as_ref(), &block_pred)
            })
            // configuration of the max size of buffered futures
            .buffered(std::cmp::min(10, segment_num))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten();

        Ok(res.collect())
    }

    #[inline]
    fn filter_segment(segment_info: &SegmentInfo, pred: &Pred) -> Result<Vec<BlockMeta>> {
        if pred(&segment_info.summary.col_stats)? {
            let block_num = segment_info.blocks.len();
            segment_info.blocks.iter().try_fold(
                Vec::with_capacity(block_num),
                |mut acc, block_meta| {
                    if pred(&block_meta.col_stats)? {
                        acc.push(block_meta.clone())
                    }
                    Ok(acc)
                },
            )
        } else {
            Ok(vec![])
        }
    }
}
