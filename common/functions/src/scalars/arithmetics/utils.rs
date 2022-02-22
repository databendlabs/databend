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
use common_exception::Result;
use num::cast::AsPrimitive;
use strength_reduce::StrengthReducedU16;
use strength_reduce::StrengthReducedU32;
use strength_reduce::StrengthReducedU64;
use strength_reduce::StrengthReducedU8;

// https://github.com/jorgecarleitao/arrow2/blob/main/src/compute/arithmetics/basic/rem.rs#L95
pub fn rem_scalar<L, R, O>(
    lhs: &<L as Scalar>::ColumnType,
    rhs: &R,
) -> Result<<O as Scalar>::ColumnType>
where
    L: PrimitiveType + AsPrimitive<R>,
    R: PrimitiveType + AsPrimitive<O> + Rem<Output = R> + ToDataType,
    O: PrimitiveType,
    u8: AsPrimitive<O>,
    u16: AsPrimitive<O>,
    u32: AsPrimitive<O>,
    u64: AsPrimitive<O>,
{
    let rhs = *rhs;
    match R::to_data_type().data_type_id() {
        TypeID::UInt64 => {
            let rhs = rhs.to_u64().unwrap();
            let reduced_rem = StrengthReducedU64::new(rhs);
            let it = lhs
                .scalar_iter()
                .map(|lhs| (lhs.to_owned_scalar().to_u64().unwrap() % reduced_rem).as_());
            Ok(<O as Scalar>::ColumnType::from_owned_iterator(it))
        }
        TypeID::UInt32 => {
            let rhs = rhs.to_u32().unwrap();
            let reduced_rem = StrengthReducedU32::new(rhs);
            let it = lhs
                .scalar_iter()
                .map(|lhs| (lhs.to_owned_scalar().to_u32().unwrap() % reduced_rem).as_());
            Ok(<O as Scalar>::ColumnType::from_owned_iterator(it))
        }
        TypeID::UInt16 => {
            let rhs = rhs.to_u16().unwrap();
            let reduced_rem = StrengthReducedU16::new(rhs);
            let it = lhs
                .scalar_iter()
                .map(|lhs| (lhs.to_owned_scalar().to_u16().unwrap() % reduced_rem).as_());
            Ok(<O as Scalar>::ColumnType::from_owned_iterator(it))
        }
        TypeID::UInt8 => {
            let rhs = rhs.to_u8().unwrap();
            let reduced_rem = StrengthReducedU8::new(rhs);
            let it = lhs
                .scalar_iter()
                .map(|lhs| (lhs.to_owned_scalar().to_u8().unwrap() % reduced_rem).as_());
            Ok(<O as Scalar>::ColumnType::from_owned_iterator(it))
        }
        _ => {
            let it = lhs
                .scalar_iter()
                .map(|lhs| (lhs.to_owned_scalar().as_() % rhs).as_());
            Ok(<O as Scalar>::ColumnType::from_owned_iterator(it))
        }
    }
}
