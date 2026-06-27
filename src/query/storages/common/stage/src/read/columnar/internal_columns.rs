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

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberColumnBuilder;

pub fn add_internal_columns(
    internal_columns: &[InternalColumnType],
    path: String,
    b: &mut DataBlock,
    start_row: &mut u64,
) {
    add_internal_columns_with_meta(internal_columns, path, b, start_row, None, None);
}

pub fn add_internal_columns_with_meta(
    internal_columns: &[InternalColumnType],
    path: String,
    b: &mut DataBlock,
    start_row: &mut u64,
    content_key: Option<&str>,
    last_modified: Option<DateTime<Utc>>,
) {
    let num_rows = b.num_rows();
    for c in internal_columns {
        match c {
            InternalColumnType::FileName | InternalColumnType::FilePath => {
                b.add_const_column(Scalar::String(path.clone()), DataType::String);
            }
            InternalColumnType::FileBasename => {
                let basename = path.rsplit('/').next().unwrap_or(&path).to_string();
                b.add_const_column(Scalar::String(basename), DataType::String);
            }
            InternalColumnType::FileRowNumber => {
                let end_row = (*start_row) + num_rows as u64;
                b.add_column(Column::Number(
                    NumberColumnBuilder::UInt64(((*start_row)..end_row).collect()).build(),
                ));
                *start_row = end_row;
            }
            InternalColumnType::FileContentKey => {
                let scalar = match content_key {
                    Some(key) => Scalar::String(key.to_string()),
                    None => Scalar::Null,
                };
                b.add_const_column(scalar, DataType::Nullable(Box::new(DataType::String)));
            }
            InternalColumnType::FileLastModified => {
                let scalar = match last_modified {
                    Some(ts) => Scalar::Timestamp(ts.timestamp_micros()),
                    None => Scalar::Null,
                };
                b.add_const_column(scalar, DataType::Nullable(Box::new(DataType::Timestamp)));
            }
            _ => {
                unreachable!("unexpected InternalColumnType in stage file reader");
            }
        }
    }
}
