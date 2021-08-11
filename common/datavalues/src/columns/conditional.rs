// Copyright 2020 Datafuse Labs.
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

use crate::columns::DataColumn;
use crate::prelude::*;

macro_rules! try_get_if_result {
    ($left:expr, $right:expr, $cond:ident, $method:ident, $len:ident) => {{
        let res: DataColumn = $left
            .$method()
            .unwrap()
            .if_then_else($right.$method().unwrap(), $cond)?
            .into_series()
            .into();
        Ok(res.resize_constant($len))
    }};
}

impl DataColumn {
    pub fn if_then_else(&self, lhs: &DataColumn, right: &DataColumn) -> Result<DataColumn> {
        let cond = self.to_minimal_array()?;
        let left = lhs.to_minimal_array()?;
        let right = right.to_minimal_array()?;

        let cond = cond.cast_with_type(&DataType::Boolean)?;
        let cond = cond.bool()?;

        let dtype = aggregate_types(&vec![lhs.data_type(), right.data_type()])?;
        let mut left = left.clone();
        if left.data_type() != dtype {
            left = left.cast_with_type(&dtype)?;
        }
        let mut right = right.clone();
        if right.data_type() != dtype {
            right = right.cast_with_type(&dtype)?;
        }

        let len = self.len();
        match left.data_type() {
            DataType::Null => Ok(lhs.resize_constant(lhs.len())),
            DataType::Boolean => try_get_if_result! {left, right, cond, bool, len},
            DataType::UInt8 => try_get_if_result! {left, right, cond, u8, len},
            DataType::UInt16 => try_get_if_result! {left, right, cond, u16, len},
            DataType::UInt32 => try_get_if_result! {left, right, cond, u32, len},
            DataType::UInt64 => try_get_if_result! {left, right, cond, u64, len},
            DataType::Int8 => try_get_if_result! {left, right, cond, i8, len},
            DataType::Int16 => try_get_if_result! {left, right, cond, i16, len},
            DataType::Int32 => try_get_if_result! {left, right, cond, i32, len},
            DataType::Int64 => try_get_if_result! {left, right, cond, i64, len},
            DataType::Float32 => try_get_if_result! {left, right, cond, f32, len},
            DataType::Float64 => try_get_if_result! {left, right, cond, f64, len},
            DataType::Date32 => try_get_if_result! {left, right, cond, date32, len},
            DataType::Date64 => try_get_if_result! {left, right, cond, date64, len},
            DataType::Utf8 => try_get_if_result! {left, right, cond, utf8, len},
            DataType::Binary => try_get_if_result! {left, right, cond, binary, len},
            _ => unimplemented!(),
        }
    }
}
