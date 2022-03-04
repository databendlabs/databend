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

use common_arrow::arrow::compute::comparison;
use common_arrow::arrow::compute::comparison::Simd8PartialEq;
use common_datavalues::prelude::*;
use common_datavalues::with_match_physical_primitive_type;
use num::traits::AsPrimitive;

use super::comparison::ComparisonFunctionCreator;
use super::comparison::ComparisonImpl;
use super::utils::compare_op;
use super::utils::compare_op_scalar;
use crate::scalars::EvalContext;

#[derive(Clone)]
pub struct ComparisonEqImpl;

impl ComparisonImpl for ComparisonEqImpl {
    fn eval_primitive<L, R, M>(l: L::RefType<'_>, r: R::RefType<'_>, _ctx: &mut EvalContext) -> bool
    where
        L: PrimitiveType + AsPrimitive<M>,
        R: PrimitiveType + AsPrimitive<M>,
        M: PrimitiveType,
    {
        l.to_owned_scalar().as_().eq(&r.to_owned_scalar().as_())
    }

    fn eval_binary(l: &[u8], r: &[u8], _ctx: &mut EvalContext) -> bool {
        l == r
    }

    fn eval_bool(l: bool, r: bool, _ctx: &mut EvalContext) -> bool {
        !(l ^ r)
    }
}

pub type ComparisonEqFunction = ComparisonFunctionCreator<ComparisonEqImpl>;

/// Perform `lhs == rhs` operation on two arrays.
pub fn primitive_eq<T>(lhs: &PrimitiveColumn<T>, rhs: &PrimitiveColumn<T>) -> BooleanColumn
where
    T: PrimitiveType + comparison::Simd8,
    T::Simd: comparison::Simd8PartialEq,
{
    compare_op(lhs, rhs, |a, b| a.eq(b))
}

/// Perform `left == right` operation on an array and a scalar value.
pub fn eq_scalar<T>(lhs: &PrimitiveColumn<T>, rhs: T) -> BooleanColumn
where
    T: PrimitiveType + comparison::Simd8,
    T::Simd: comparison::Simd8PartialEq,
{
    compare_op_scalar(lhs, rhs, |a, b| a.eq(b))
}

/// `==` between two [`Array`]s.
/// Use [`can_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn eq(lhs: &ColumnRef, rhs: &ColumnRef) -> BooleanColumn {
    assert_eq!(lhs.data_type_id(), rhs.data_type_id());

    use PhysicalTypeID::*;
    let physical_id = lhs.data_type_id().to_physical_type();
    with_match_physical_primitive_type!(physical_id, |$T| {
        let lhs: &PrimitiveColumn<$T> = lhs.as_any().downcast_ref().unwrap();
        let rhs: &PrimitiveColumn<$T> = rhs.as_any().downcast_ref().unwrap();
        primitive_eq::<$T>(lhs, rhs)
    },{
        match physical_id {
            Boolean => todo!(),
            _ => todo!(
                "Comparison between {:?} are not yet supported",
                lhs.data_type_id()
            ),
        }
    })
}

/*

macro_rules! compare {
    ($lhs:expr, $rhs:expr, $op:tt, $p:tt) => {{
        let lhs = $lhs;
        let rhs = $rhs;
        assert_eq!(
            lhs.data_type().to_logical_type(),
            rhs.data_type().to_logical_type()
        );

        use crate::datatypes::PhysicalType::*;
        match lhs.data_type().to_physical_type() {
            Boolean => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                boolean::$op(lhs, rhs)
            }
            Primitive(primitive) => $p!(primitive, |$T| {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<$T>(lhs, rhs)
            }),
            Utf8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                utf8::$op::<i32>(lhs, rhs)
            }
            LargeUtf8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                utf8::$op::<i64>(lhs, rhs)
            }
            Binary => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                binary::$op::<i32>(lhs, rhs)
            }
            LargeBinary => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                binary::$op::<i64>(lhs, rhs)
            }
            _ => todo!(
                "Comparison between {:?} are not yet supported",
                lhs.data_type()
            ),
        }
    }};
}

*/
