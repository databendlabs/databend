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

use std::ops::Rem;

use common_datavalues::prelude::*;
use num::cast::AsPrimitive;
use strength_reduce::StrengthReducedU16;
use strength_reduce::StrengthReducedU32;
use strength_reduce::StrengthReducedU64;
use strength_reduce::StrengthReducedU8;

pub fn validate_input<'a>(
    col0: &'a DataColumnWithField,
    col1: &'a DataColumnWithField,
) -> (&'a DataColumnWithField, &'a DataColumnWithField) {
    if col0.data_type().is_integer() || col0.data_type().is_interval() {
        (col0, col1)
    } else {
        (col1, col0)
    }
}

// https://github.com/jorgecarleitao/arrow2/blob/main/src/compute/arithmetics/basic/rem.rs#L95
pub fn rem_scalar<T, D, R>(lhs: &DFPrimitiveArray<T>, rhs: &D) -> DFPrimitiveArray<R>
where
    T: DFPrimitiveType + AsPrimitive<D>,
    D: DFPrimitiveType + AsPrimitive<R> + Rem<Output = D>,
    R: DFPrimitiveType,
    u8: AsPrimitive<R>,
    u16: AsPrimitive<R>,
    u32: AsPrimitive<R>,
    u64: AsPrimitive<R>,
{
    let rhs = *rhs;
    match D::data_type() {
        DataType::UInt64 => {
            let rhs = rhs.to_u64().unwrap();
            let reduced_rem = StrengthReducedU64::new(rhs);
            unary(lhs, |a| {
                AsPrimitive::<R>::as_(a.to_u64().unwrap() % reduced_rem)
            })
        }
        DataType::UInt32 => {
            let rhs = rhs.to_u32().unwrap();
            let reduced_rem = StrengthReducedU32::new(rhs);
            unary(lhs, |a| {
                AsPrimitive::<R>::as_(a.to_u32().unwrap() % reduced_rem)
            })
        }
        DataType::UInt16 => {
            let rhs = rhs.to_u16().unwrap();
            let reduced_rem = StrengthReducedU16::new(rhs);
            unary(lhs, |a| {
                AsPrimitive::<R>::as_(a.to_u16().unwrap() % reduced_rem)
            })
        }
        DataType::UInt8 => {
            let rhs = rhs.to_u8().unwrap();
            let reduced_rem = StrengthReducedU8::new(rhs);
            unary(lhs, |a| {
                AsPrimitive::<R>::as_(a.to_u8().unwrap() % reduced_rem)
            })
        }
        _ => unary(lhs, |a| {
            let a: D = a.as_();
            AsPrimitive::<R>::as_(a % rhs)
        }),
    }
}
