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
use common_expression::Chunk;
use common_expression::Column;
use common_expression::DataSchemaRef;

use crate::field_encoder::FieldEncoderRowBased;
use crate::field_encoder::FieldEncoderValues;
use crate::output_format::OutputFormat;

pub struct ValuesOutputFormat {
    field_encoder: FieldEncoderValues,
}

impl ValuesOutputFormat {
    #[allow(unused)]
    pub fn create(_schema: DataSchemaRef, field_encoder: FieldEncoderValues) -> Self {
        Self { field_encoder }
    }
}

impl OutputFormat for ValuesOutputFormat {
    fn serialize_chunk(&mut self, chunk: &Chunk) -> Result<Vec<u8>> {
        let rows_size = chunk.num_rows();
        let mut buf = Vec::with_capacity(chunk.memory_size());

        let columns: Vec<Column> = chunk
            .convert_to_full()
            .columns()
            .map(|column| column.value.clone().into_column().unwrap())
            .collect();

        for row_index in 0..rows_size {
            if row_index != 0 {
                buf.push(b',');
            }
            buf.push(b'(');
            for (i, column) in columns.iter().enumerate() {
                if i != 0 {
                    buf.push(b',');
                }
                self.field_encoder
                    .write_field(column, row_index, &mut buf, true);
            }
            buf.push(b')');
        }
        Ok(buf)
    }

    fn finalize(&mut self) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}
