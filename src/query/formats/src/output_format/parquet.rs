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

use common_exception::Result;
use common_expression::serialize_to_parquet;
use common_expression::Chunk;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;

use crate::output_format::OutputFormat;
use crate::FileFormatOptionsExt;

#[derive(Default)]
pub struct ParquetOutputFormat {
    schema: TableSchemaRef,
    chunks: Vec<Chunk>,
}

impl ParquetOutputFormat {
    pub fn create(schema: TableSchemaRef, _options: &FileFormatOptionsExt) -> Self {
        Self {
            schema,
            chunks: vec![],
        }
    }
}

impl OutputFormat for ParquetOutputFormat {
    fn serialize_chunk(&mut self, chunk: &Chunk) -> Result<Vec<u8>> {
        self.chunks.push(chunk.clone());
        Ok(vec![])
    }

    fn buffer_size(&mut self) -> usize {
        self.chunks.iter().map(|b| b.memory_size()).sum()
    }

    fn finalize(&mut self) -> Result<Vec<u8>> {
        let chunks = std::mem::take(&mut self.chunks);
        if chunks.is_empty() {
            return Ok(vec![]);
        }
        let mut buf = Vec::with_capacity(100 * 1024 * 1024);
        let _ = serialize_to_parquet(chunks,  &self.schema, &mut buf)?;
        Ok(buf)
    }
}
