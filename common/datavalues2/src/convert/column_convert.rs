// Copyright 2021 Datafuse Labs.
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
// limitations under the License.pub use data_type::*;

use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_datavalues::prelude::DataColumn as OldDataColumn;

use crate::ColumnRef;
use crate::ConstColumn;
use crate::IntoColumn;
use crate::NullableColumn;

pub fn convert2_new_column(column: &OldDataColumn, nullable: bool) -> ColumnRef {
    let result = convert2_new_column_nonull(column);
    if nullable {
        let arrow_c = column.get_array_ref().unwrap();
        let bitmap = arrow_c.validity().cloned();

        let bitmap = if let Some(b) = bitmap {
            b
        } else {
            let mut b = MutableBitmap::with_capacity(arrow_c.len());
            b.extend_constant(arrow_c.len(), true);
            b.into()
        };

        let column = NullableColumn::new(result, bitmap);
        return Arc::new(column);
    }

    result
}

pub fn convert2_old_column(column: &ColumnRef) -> OldDataColumn {
    let arrow_c = column.as_arrow_array();

    OldDataColumn::from(arrow_c)
}

fn convert2_new_column_nonull(column: &OldDataColumn) -> ColumnRef {
    match column {
        OldDataColumn::Array(array) => {
            let arrow_column = array.get_array_ref();

            arrow_column.into_column()
        }
        OldDataColumn::Constant(value, size) => {
            let s = value.to_series_with_size(1).unwrap();
            let arrow_column = s.get_array_ref();
            let col = arrow_column.into_column();

            Arc::new(ConstColumn::new(col, *size))
        }
    }
}
