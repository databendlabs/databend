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
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::formats::output_format::OutputFormat;
use crate::storages::fuse::io::serialize_data_blocks;

#[derive(Default)]
pub struct ParquetOutputFormat {
    schema: DataSchemaRef,
    data_blocks: Vec<DataBlock>,
}

impl ParquetOutputFormat {
    pub fn create(schema: DataSchemaRef) -> Self {
        Self {
            schema,
            data_blocks: vec![],
        }
    }
}

impl OutputFormat for ParquetOutputFormat {
    fn serialize_block(&mut self, block: &DataBlock, _format: &FormatSettings) -> Result<Vec<u8>> {
        self.data_blocks.push(block.clone());
        Ok(vec![])
    }

    fn finalize(&mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(100 * 1024 * 1024);
        let _ = serialize_data_blocks(self.data_blocks.clone(), &self.schema, &mut buf)?;

        Ok(buf)
    }
}
