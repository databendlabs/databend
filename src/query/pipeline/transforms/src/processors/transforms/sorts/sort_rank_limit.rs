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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;

use crate::processors::AccumulatingTransform;

pub struct TransformRankLimitSort {
    limit: LimitType,
    batch_rows: usize,
    sort_desc: Arc<[SortColumnDescription]>,
    blocks: DataBlockVec,
    rows: usize,
}

impl TransformRankLimitSort {
    pub fn new(limit: usize, sort_desc: Arc<[SortColumnDescription]>, batch_rows: usize) -> Self {
        Self {
            limit: LimitType::LimitRank(limit),
            batch_rows,
            sort_desc,
            blocks: DataBlockVec::default(),
            rows: 0,
        }
    }

    fn flush_pending(&mut self) -> Result<Option<DataBlock>> {
        if self.blocks.block_rows().is_empty() {
            return Ok(None);
        }

        let sorted = self.blocks.sort_limit(self.sort_desc.clone(), self.limit)?;
        self.blocks.clear();

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
