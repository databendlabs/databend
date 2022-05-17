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
use common_datavalues::DataType;
use common_datavalues::TypeSerializer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::formats::output_format::OutputFormat;

const FIELD_DELIMITER: u8 = b'\t';
const ROW_DELIMITER: u8 = b'\n';

pub type TSVOutputFormat = TCSVOutputFormat<true>;
pub type CSVOutputFormat = TCSVOutputFormat<false>;

#[derive(Default)]
pub struct TCSVOutputFormat<const TSV: bool> {}

impl<const TSV: bool> OutputFormat for TCSVOutputFormat<TSV> {
    fn serialize_block(&mut self, block: &DataBlock, format: &FormatSettings) -> Result<Vec<u8>> {
        let rows_size = block.column(0).len();
        let columns_size = block.num_columns();

        let mut buf = Vec::with_capacity(block.memory_size());
        let mut col_table = Vec::new();
        for col_index in 0..columns_size {
            let column = block.column(col_index);
            let column = column.convert_full_column();
            let field = block.schema().field(col_index);
            let data_type = field.data_type();
            let serializer = data_type.create_serializer();
            col_table.push(serializer.serialize_column(&column, format).map_err(|e| {
                ErrorCode::UnexpectedError(format!(
                    "fail to serialize field {}, error = {}",
                    field.name(),
                    e
                ))
            })?);
        }

        let fd = if TSV {
            FIELD_DELIMITER
        } else {
            format.field_delimiter[0]
        };

        let rd = if TSV {
            ROW_DELIMITER
        } else {
            format.record_delimiter[0]
        };

        for row_index in 0..rows_size {
            for col in col_table.iter().take(columns_size - 1) {
                buf.extend_from_slice(col[row_index].as_bytes());
                buf.push(fd);
            }
            buf.extend_from_slice(col_table[columns_size - 1][row_index].as_bytes());
            buf.push(rd);
        }

        Ok(buf)
    }

    fn finalize(&mut self) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}
