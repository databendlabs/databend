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
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_table_meta::table::TableCompression;

use crate::output_format::OutputFormat;
use crate::FileFormatOptionsExt;

#[derive(Default)]
pub struct ParquetOutputFormat {
    schema: TableSchemaRef,
    data_blocks: Vec<DataBlock>,
}

impl ParquetOutputFormat {
    pub fn create(schema: TableSchemaRef, _options: &FileFormatOptionsExt) -> Self {
        Self {
            schema,
            data_blocks: vec![],
        }
    }
}

impl OutputFormat for ParquetOutputFormat {
    fn serialize_block(&mut self, block: &DataBlock) -> Result<Vec<u8>> {
        self.data_blocks.push(block.clone());
        Ok(vec![])
    }

    fn buffer_size(&mut self) -> usize {
        self.data_blocks.iter().map(|b| b.memory_size()).sum()
    }

    fn finalize(&mut self) -> Result<Vec<u8>> {
        let blocks = std::mem::take(&mut self.data_blocks);
        if blocks.is_empty() {
            return Ok(vec![]);
        }
        let mut buf = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let _ = blocks_to_parquet(
            &self.schema,
            blocks,
            &mut buf,
            TableCompression::Zstd,
            &Default::default(),
        )?;
        Ok(buf)
    }
}
