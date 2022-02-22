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
// limitations under the License.

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn filter_block(block: &DataBlock, predicate: &ColumnRef) -> Result<DataBlock> {
        if block.num_columns() == 0 || block.num_rows() == 0 {
            return Ok(block.clone());
        }

        let predict_boolean_nonull = Self::cast_to_nonull_boolean(predicate)?;
        // faster path for constant filter
        if predict_boolean_nonull.is_const() {
            let flag = predict_boolean_nonull.get_bool(0)?;
            if flag {
                return Ok(block.clone());
            } else {
                return Ok(DataBlock::empty_with_schema(block.schema().clone()));
            }
        }

        let boolean_col: &BooleanColumn = Series::check_get(&predict_boolean_nonull)?;
        let rows = boolean_col.len();
        let count_zeros = boolean_col.values().null_count();
        match count_zeros {
            0 => Ok(block.clone()),
            _ => {
                if count_zeros == rows {
                    return Ok(DataBlock::empty_with_schema(block.schema().clone()));
                }
                let mut after_columns = Vec::with_capacity(block.num_columns());
                for data_column in block.columns() {
                    after_columns.push(data_column.filter(boolean_col));
                }

                Ok(DataBlock::create(block.schema().clone(), after_columns))
            }
        }
    }

    pub fn cast_to_nonull_boolean(predict: &ColumnRef) -> Result<ColumnRef> {
        if predict.is_const() {
            let col: &ConstColumn = unsafe { Series::static_cast(predict) };
            let inner_boolean = Self::cast_to_nonull_boolean(col.inner())?;
            Ok(Arc::new(ConstColumn::new(inner_boolean, col.len())))
        } else if predict.is_nullable() {
            let col: &NullableColumn = unsafe { Series::static_cast(predict) };
            let inner_boolean = Self::cast_to_nonull_boolean(col.inner())?;
            // no const nullable or nullable constant
            let inner: &BooleanColumn = Series::check_get(&inner_boolean)?;
            let validity = col.ensure_validity();
            let values = combine_validities(Some(validity), Some(inner.values())).unwrap();

            let col = BooleanColumn::from_arrow_data(values);
            Ok(Arc::new(col))
        } else if predict.data_type_id() == TypeID::Null {
            Ok(Arc::new(ConstColumn::new(
                Series::from_data(vec![false]),
                predict.len(),
            )))
        } else {
            let data_type_id = predict.data_type_id();
            if data_type_id == TypeID::Boolean {
                return Ok(predict.clone());
            }

            with_match_primitive_type_id!(data_type_id, |$T| {
                let col: &PrimitiveColumn<$T> = unsafe { Series::static_cast(predict) };
                let iter = col.iter().map(|v| *v > $T::default());
                let col = BooleanColumn::from_owned_iterator(iter);

                return Ok(Arc::new(col));
            },
            {
                return Err(ErrorCode::BadDataValueType(format!(
                    "Filter predict column does not support type '{:?}'",
                    data_type_id
                )));
            })
        }
    }
}
