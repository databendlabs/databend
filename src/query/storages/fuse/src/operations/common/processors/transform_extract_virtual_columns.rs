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
use databend_common_pipeline_transforms::processors::Transform;

use crate::io::VirtualColumnExtractor;

/// Appends extracted virtual columns to blocks of source columns, so extraction
/// runs at the parallelism of the reader instead of the writer.
pub struct TransformExtractVirtualColumns {
    extractor: Arc<VirtualColumnExtractor>,
}

impl TransformExtractVirtualColumns {
    pub fn new(extractor: Arc<VirtualColumnExtractor>) -> Self {
        Self { extractor }
    }
}

impl Transform for TransformExtractVirtualColumns {
    const NAME: &'static str = "TransformExtractVirtualColumns";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        // A column-less empty block carries no rows to extract.
        if data_block.num_columns() == 0 && data_block.is_empty() {
            return Ok(data_block);
        }
        self.extractor.extract(data_block)
    }
}
