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

use databend_common_expression::TableSchemaRef;
use databend_common_storage::FileParseError;

pub fn truncate_column_data(s: String) -> String {
    if s.len() > 100 {
        s.chars().take(100).collect::<String>()
    } else {
        s
    }
}

pub fn get_decode_error_by_pos(
    column_index: usize,
    schema: &TableSchemaRef,
    decode_error: &str,
    column_data: &[u8],
) -> FileParseError {
    let field = &schema.fields()[column_index];
    let column_data = String::from_utf8_lossy(column_data).to_string();
    FileParseError::ColumnDecodeError {
        column_index,
        decode_error: decode_error.to_string(),
        column_name: field.name().to_string(),
        column_type: field.data_type().to_string(),
        column_data: truncate_column_data(column_data),
    }
}
