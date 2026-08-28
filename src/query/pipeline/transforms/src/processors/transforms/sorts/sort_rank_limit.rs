// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;

use crate::processors::AccumulatingTransform;

pub struct TransformRankLimitSort {
    limit: usize,
    batch_rows: usize,
    sort_desc: Arc<[SortColumnDescription]>,
    blocks: DataBlockVec,
    rows: usize,
    runtime_top_n_filter: Option<(usize, Arc<RuntimeTopNFilter>)>,
}

impl TransformRankLimitSort {
    pub fn new(
        limit: usize,
        sort_desc: Arc<[SortColumnDescription]>,
        batch_rows: usize,
        runtime_top_n_filter: Option<(usize, Arc<RuntimeTopNFilter>)>,
    ) -> Self {
        Self {
            limit,
            batch_rows,
            sort_desc,
            blocks: DataBlockVec::default(),
            rows: 0,
            runtime_top_n_filter,
        }
    }

    fn publish_runtime_top_n_boundary(&self, block: &DataBlock) {
        let Some((source_offset, filter)) = &self.runtime_top_n_filter else {
            return;
        };
        if self.limit == 0 {
            return;
        }

        let column = block.get_by_offset(*source_offset);
        let mut previous = None;
        let mut rank = 0;
        for row in 0..block.num_rows() {
            let Some(value) = column.index(row) else {
                continue;
            };
            let value = value.to_owned();
            if previous.as_ref() == Some(&value) {
                continue;
            }

            rank += 1;
            if rank == self.limit {
                filter.update(&value);
                return;
            }
            previous = Some(value);
        }
    }

    fn flush_pending(&mut self) -> Result<Option<DataBlock>> {
        if self.blocks.block_rows().is_empty() {
            return Ok(None);
        }

        let sorted = self
            .blocks
            .sort_limit(self.sort_desc.clone(), LimitType::LimitRank(self.limit))?;
        self.blocks.clear();
        self.rows = 0;
        self.publish_runtime_top_n_boundary(&sorted);

        Ok(Some(sorted))
    }
}

impl AccumulatingTransform for TransformRankLimitSort {
    const NAME: &'static str = "TransformRankLimitSort";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        self.rows += data.num_rows();
        self.blocks.push(data)?;

        if self.rows >= self.batch_rows {
            return Ok(self.flush_pending()?.into_iter().collect());
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output {
            Ok(self.flush_pending()?.into_iter().collect())
        } else {
            self.blocks.clear();
            self.rows = 0;
            Ok(vec![])
        }
    }
}
