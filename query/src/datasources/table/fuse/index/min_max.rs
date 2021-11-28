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

use common_dal::DataAccessor;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Extras;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::datasources::index::RangeFilter;
use crate::datasources::table::fuse::io::snapshot_location;
use crate::datasources::table::fuse::meta::BlockMeta;
use crate::datasources::table::fuse::meta::SegmentInfo;
use crate::datasources::table::fuse::meta::TableSnapshot;
use crate::datasources::table::fuse::statistics::BlockStats;

pub struct MinMaxIndex {
    table_snapshot_loc: String,
    da: Arc<dyn DataAccessor>,
}

type Pred = Box<dyn Fn(&BlockStats) -> Result<bool> + Send + Sync + Unpin>;
impl MinMaxIndex {
    pub fn new(table_snapshot: &TableSnapshot, da: Arc<dyn DataAccessor>) -> Self {
        Self {
            table_snapshot_loc: snapshot_location(
                table_snapshot.snapshot_id.to_simple().to_string(),
            ),
            da,
        }
    }

    // Returns an iterator or stream would be better
    pub async fn apply(
        &self,
        schema: DataSchemaRef,
        push_down: Option<Extras>,
    ) -> Result<Vec<BlockMeta>> {
        let block_pred: Pred = match push_down {
            Some(exprs) if !exprs.filters.is_empty() => {
                // for the time being, we only handle the first expr
                let verifiable_expression = RangeFilter::try_create(&exprs.filters[0], schema)?;
                Box::new(move |v: &BlockStats| verifiable_expression.eval(v))
            }
            _ => Box::new(|_: &BlockStats| Ok(true)),
        };

        let snapshot =
            common_dal::read_obj::<TableSnapshot>(self.da.clone(), self.table_snapshot_loc.clone())
                .await?;
        let segment_num = snapshot.segments.len();
        let segment_locs = snapshot.segments;

        if segment_locs.is_empty() {
            return Ok(vec![]);
        };

        let res = futures::stream::iter(segment_locs)
            .map(|seg_loc| async {
                let segment_info =
                    common_dal::read_obj::<SegmentInfo>(self.da.clone(), seg_loc).await?;
                Self::filter_segment(segment_info, &block_pred)
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
    fn filter_segment(segment_info: SegmentInfo, pred: &Pred) -> Result<Vec<BlockMeta>> {
        if pred(&segment_info.summary.col_stats)? {
            let block_num = segment_info.blocks.len();
            segment_info.blocks.into_iter().try_fold(
                Vec::with_capacity(block_num),
                |mut acc, block_meta| {
                    if pred(&block_meta.col_stats)? {
                        acc.push(block_meta)
                    }
                    Ok(acc)
                },
            )
        } else {
            Ok(vec![])
        }
    }
}

pub async fn range_filter(
    table_snapshot: &TableSnapshot,
    schema: DataSchemaRef,
    push_down: Option<Extras>,
    data_accessor: Arc<dyn DataAccessor>,
) -> common_exception::Result<Vec<BlockMeta>> {
    let range_index = MinMaxIndex::new(table_snapshot, data_accessor);
    range_index.apply(schema, push_down).await
}
