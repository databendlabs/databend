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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::sort_merge;
use databend_common_settings::Settings;

pub trait PartitionProcessStrategy: Send + Sync + 'static {
    const NAME: &'static str;

    fn process_data_blocks(&self, data_blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>>;
}

pub struct WindowPartitionStrategy {
    sort_desc: Vec<SortColumnDescription>,
    schema: DataSchemaRef,
    max_block_size: usize,
    sort_spilling_batch_bytes: usize,
    enable_loser_tree: bool,
    have_order_col: bool,
}

impl WindowPartitionStrategy {
    pub fn try_create(
        settings: &Settings,
        sort_desc: Vec<SortColumnDescription>,
        schema: DataSchemaRef,
        have_order_col: bool,
    ) -> Result<Self> {
        let max_block_size = settings.get_max_block_size()? as usize;
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;
        let sort_spilling_batch_bytes = settings.get_sort_spilling_batch_bytes()?;
        Ok(Self {
            sort_desc,
            schema,
            max_block_size,
            sort_spilling_batch_bytes,
            enable_loser_tree,
            have_order_col,
        })
    }
}

impl PartitionProcessStrategy for WindowPartitionStrategy {
    const NAME: &'static str = "Window";

    fn process_data_blocks(&self, data_blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let data_blocks = data_blocks
            .into_iter()
            .map(|data_block| DataBlock::sort(&data_block, &self.sort_desc, None))
            .collect::<Result<Vec<_>>>()?;

        sort_merge(
            self.schema.clone(),
            self.max_block_size,
            self.sort_desc.clone(),
            data_blocks,
            self.sort_spilling_batch_bytes,
            self.enable_loser_tree,
            self.have_order_col,
        )
    }
}
