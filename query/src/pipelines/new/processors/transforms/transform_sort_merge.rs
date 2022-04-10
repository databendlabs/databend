// Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datablocks::SortColumnDescription;
use common_exception::Result;

use super::Compactor;
use super::TransformCompact;

pub struct SortMergeCompactor {
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,
}

impl SortMergeCompactor {
    pub fn new(
        limit: Option<usize>,
        sort_columns_descriptions: Vec<SortColumnDescription>,
    ) -> Self {
        SortMergeCompactor {
            limit,
            sort_columns_descriptions,
        }
    }
}

impl Compactor for SortMergeCompactor {
    fn name() -> &'static str {
        "SortMergeTransform"
    }

    fn compact(&self, blocks: &Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let block =
            DataBlock::merge_sort_blocks(&blocks, &self.sort_columns_descriptions, self.limit)?;
        Ok(vec![block])
    }
}

pub type TransformSortMerge = TransformCompact<SortMergeCompactor>;
