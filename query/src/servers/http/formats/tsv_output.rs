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

const FIELD_DELIMITER: u8 = b'\t';
const ROW_DELIMITER: u8 = b'\n';

pub fn block_to_tsv(block: &DataBlock, format: &FormatSettings) -> Result<Vec<u8>> {
    let rows_size = block.column(0).len();
    let columns_size = block.num_columns();

    let mut col_table = Vec::new();
    for col_index in 0..columns_size {
        let column = block.column(col_index);
        let column = column.convert_full_column();
        let field = block.schema().field(col_index);
        let data_type = field.data_type();
        let serializer = data_type.create_serializer();
        // todo(youngsofun): escape
        col_table.push(serializer.serialize_column(&column, format).map_err(|e| {
            ErrorCode::UnexpectedError(format!(
                "fail to serialize filed {}, error = {}",
                field.name(),
                e
            ))
        })?);
    }
    let mut buf = vec![];
    for row_index in 0..rows_size {
        for col in col_table.iter().take(columns_size - 1) {
            buf.extend_from_slice(col[row_index].as_bytes());
            buf.push(FIELD_DELIMITER);
        }
        buf.extend_from_slice(col_table[columns_size - 1][row_index].as_bytes());
        buf.push(ROW_DELIMITER);
    }
    Ok(buf)
}
