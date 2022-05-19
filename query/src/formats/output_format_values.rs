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
use common_datavalues::DataType;
use common_datavalues::TypeSerializer;
use common_datavalues::TypeSerializerImpl;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::formats::output_format::OutputFormat;

#[derive(Default)]
pub struct ValuesOutputFormat {
    serializers: Vec<TypeSerializerImpl>,
}

impl ValuesOutputFormat {
    pub fn create(schema: DataSchemaRef) -> Self {
        let serializers: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().create_serializer())
            .collect();

        Self { serializers }
    }
}

impl OutputFormat for ValuesOutputFormat {
    fn serialize_block(&mut self, block: &DataBlock, format: &FormatSettings) -> Result<Vec<u8>> {
        let rows_size = block.column(0).len();
        let columns_size = block.num_columns();

        assert_eq!(self.serializers.len(), columns_size);

        let mut buf = Vec::with_capacity(block.memory_size());
        let mut col_table = Vec::new();
        for col_index in 0..columns_size {
            let column = block.column(col_index).convert_full_column();
            let res = self.serializers[col_index].serialize_column_quoted(&column, format)?;
            col_table.push(res)
        }

        for row_index in 0..rows_size {
            if row_index != 0 {
                buf.push(b',');
            }
            buf.push(b'(');
            for (i, col) in col_table.iter().enumerate() {
                if i != 0 {
                    buf.push(b',');
                }
                buf.extend_from_slice(col[row_index].as_bytes());
            }
            buf.push(b')');
        }
        Ok(buf)
    }

    fn finalize(&mut self) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}
