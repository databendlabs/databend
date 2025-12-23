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
    for c in internal_columns {
        match c {
            InternalColumnType::FileName => {
                b.add_const_column(Scalar::String(path.clone()), DataType::String);
            }
            InternalColumnType::FileRowNumber => {
                let end_row = (*start_row) + b.num_rows() as u64;
                b.add_column(Column::Number(
                    NumberColumnBuilder::UInt64(((*start_row)..end_row).collect()).build(),
                ));
                *start_row = end_row;
            }
            _ => {
                unreachable!(
                    "except InternalColumnType::FileName or InternalColumnType::FileRowNumber"
                );
            }
        }
    }
}
