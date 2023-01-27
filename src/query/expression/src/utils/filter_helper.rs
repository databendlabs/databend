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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::arrow::bitmap_into_mut;
use crate::types::number::NumberScalar;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::NumberColumn;
use crate::with_number_mapped_type;
use crate::Column;
use crate::Scalar;
use crate::Value;

pub struct FilterHelpers;

impl FilterHelpers {
    // check if the predicate has any valid row
    pub fn filter_exists(predicate: &Value<AnyType>) -> Result<bool> {
        let predicate = Self::cast_to_nonull_boolean(predicate).ok_or_else(|| {
            ErrorCode::BadDataValueType(format!(
                "Filter predict column does not support type '{:?}'",
                predicate
            ))
        })?;
        match predicate {
            Value::Scalar(s) => Ok(s),
            Value::Column(bitmap) => Ok(bitmap.len() != bitmap.unset_bits()),
        }
    }

    // Must be numeric, boolean, or string value type
    #[inline]
    pub fn cast_to_nonull_boolean(predicate: &Value<AnyType>) -> Option<Value<BooleanType>> {
        match predicate {
            Value::Scalar(s) => Self::cast_scalar_to_boolean(s).map(Value::Scalar),
            Value::Column(c) => Self::cast_column_to_boolean(c).map(Value::Column),
        }
    }

    #[inline]
    pub fn is_all_unset(predicate: &Value<BooleanType>) -> bool {
        match &predicate {
            Value::Scalar(v) => !v,
            Value::Column(bitmap) => bitmap.unset_bits() == bitmap.len(),
        }
    }

    fn cast_scalar_to_boolean(s: &Scalar) -> Option<bool> {
        match s {
            Scalar::Number(num) => with_number_mapped_type!(|SRC_TYPE| match num {
                NumberScalar::SRC_TYPE(value) => Some(value != &SRC_TYPE::default()),
            }),
            Scalar::Boolean(value) => Some(*value),
            Scalar::String(value) => Some(!value.is_empty()),
            Scalar::Timestamp(value) => Some(*value != 0),
            Scalar::Date(value) => Some(*value != 0),
            Scalar::Null => Some(false),
            _ => None,
        }
    }

    fn cast_column_to_boolean(c: &Column) -> Option<Bitmap> {
        match c {
            Column::Number(num) => with_number_mapped_type!(|SRC_TYPE| match num {
                NumberColumn::SRC_TYPE(value) => Some(BooleanType::column_from_iter(
                    value.iter().map(|v| v != &SRC_TYPE::default()),
                    &[],
                )),
            }),
            Column::Boolean(value) => Some(value.clone()),
            Column::String(value) => Some(BooleanType::column_from_iter(
                value.iter().map(|s| !s.is_empty()),
                &[],
            )),
            Column::Timestamp(value) => Some(BooleanType::column_from_iter(
                value.iter().map(|v| *v != 0),
                &[],
            )),
            Column::Date(value) => Some(BooleanType::column_from_iter(
                value.iter().map(|v| *v != 0),
                &[],
            )),
            Column::Null { len } => Some(MutableBitmap::from_len_zeroed(*len).into()),
            Column::Nullable(c) => {
                let inner = Self::cast_column_to_boolean(&c.column)?;
                Some((&inner) & (&c.validity))
            }
            _ => None,
        }
    }

    pub fn try_as_const_bool(value: &Value<BooleanType>) -> Result<Option<bool>> {
        match value {
            Value::Scalar(v) => Ok(Some(*v)),
            _ => Ok(None),
        }
    }

    pub fn filter_to_bitmap(predicate: Value<BooleanType>, rows: usize) -> MutableBitmap {
        match predicate {
            Value::Scalar(true) => MutableBitmap::from_len_set(rows),
            Value::Scalar(false) => MutableBitmap::from_len_zeroed(rows),
            Value::Column(bitmap) => bitmap_into_mut(bitmap),
        }
    }
}
