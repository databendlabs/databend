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

use std::fmt;
use std::marker::PhantomData;
use std::ops::Rem;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_types_error;
use common_exception::ErrorCode;
use common_exception::Result;
use num_traits::AsPrimitive;

use super::utils::rem_scalar;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticDescription;
use crate::scalars::Function;

pub struct ArithmeticModuloFunction;

impl ArithmeticModuloFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        let left_type = remove_nullable(args[0]).data_type_id();
        let right_type = remove_nullable(args[1]).data_type_id();

        with_match_primitive_types_error!(left_type, |$T| {
            with_match_primitive_types_error!(right_type, |$D| {
                Ok(Box::new(
                        ModuloFunctionImpl::<$T, $D, <($T, $D) as ResultTypeOfBinary>::LeastSuper, <($T, $D) as ResultTypeOfBinary>::Modulo>::default()
                ))
            })
        })
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

#[derive(Clone, Default)]
pub struct ModuloFunctionImpl<L, R, M, O> {
    l: PhantomData<L>,
    r: PhantomData<R>,
    m: PhantomData<M>,
    o: PhantomData<O>,
}

impl<L, R, M, O> Function for ModuloFunctionImpl<L, R, M, O>
where
    L: PrimitiveType + AsPrimitive<M>,
    R: PrimitiveType + AsPrimitive<M>,
    M: PrimitiveType + AsPrimitive<O> + Rem<Output = M> + num::Zero + ToDataType,
    O: PrimitiveType + ToDataType,
    u8: AsPrimitive<O>,
    u16: AsPrimitive<O>,
    u32: AsPrimitive<O>,
    u64: AsPrimitive<O>,
{
    fn name(&self) -> &str {
        "ModuloFunctionImpl"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(O::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let lhs = columns[0].column();
        let rhs = columns[1].column();
        match (lhs.is_const(), rhs.is_const()) {
            (false, true) => {
                let left: &<L as Scalar>::ColumnType = unsafe { Series::static_cast(lhs) };
                let right = R::try_create_viewer(rhs)?;

                let r = right.value_at(0).to_owned_scalar().as_();
                if r == M::zero() {
                    return Err(ErrorCode::BadArguments("Division by zero"));
                }
                let col = rem_scalar::<L, M, O>(left, &r)?;
                Ok(Arc::new(col))
            }
            (false, false) => {
                let left: &<L as Scalar>::ColumnType = unsafe { Series::static_cast(lhs) };
                let right: &<R as Scalar>::ColumnType = unsafe { Series::static_cast(rhs) };

                let mut col_builder = MutablePrimitiveColumn::<O>::with_capacity(lhs.len());
                for (l, r) in left.scalar_iter().zip(right.scalar_iter()) {
                    let l = l.to_owned_scalar().as_();
                    let r = r.to_owned_scalar().as_();
                    if std::intrinsics::unlikely(r == M::zero()) {
                        return Err(ErrorCode::BadArguments("Division by zero"));
                    }
                    let o = (l % r).as_();
                    col_builder.append_value(o);
                }
                Ok(col_builder.to_column())
            }
            (true, false) => {
                let left = L::try_create_viewer(lhs)?;
                let l = left.value_at(0).to_owned_scalar().as_();
                let right: &<R as Scalar>::ColumnType = unsafe { Series::static_cast(rhs) };

                let mut col_builder = MutablePrimitiveColumn::<O>::with_capacity(rhs.len());
                for r in right.scalar_iter() {
                    let r = r.to_owned_scalar().as_();
                    if std::intrinsics::unlikely(r == M::zero()) {
                        return Err(ErrorCode::BadArguments("Division by zero"));
                    }
                    let o = (l % r).as_();
                    col_builder.append_value(o);
                }
                Ok(col_builder.to_column())
            }
            (true, true) => {
                let left = L::try_create_viewer(lhs)?;
                let right = R::try_create_viewer(rhs)?;

                let mut col_builder = MutablePrimitiveColumn::<O>::with_capacity(lhs.len());
                for (l, r) in left.iter().zip(right.iter()) {
                    let l = l.to_owned_scalar().as_();
                    let r = r.to_owned_scalar().as_();
                    if r == M::zero() {
                        return Err(ErrorCode::BadArguments("Division by zero"));
                    }
                    let o = (l % r).as_();
                    col_builder.append_value(o);
                }
                Ok(col_builder.to_column())
            }
        }
    }
}

impl<L, R, M, O> fmt::Display for ModuloFunctionImpl<L, R, M, O>
where
    L: PrimitiveType + AsPrimitive<M>,
    R: PrimitiveType + AsPrimitive<M>,
    M: PrimitiveType + AsPrimitive<O> + Rem<Output = M> + num::Zero,
    O: PrimitiveType + ToDataType,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "div")
    }
}
