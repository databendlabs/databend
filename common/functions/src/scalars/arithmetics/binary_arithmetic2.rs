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
use std::ops::Add;
use std::sync::Arc;

use common_datavalues2::arithmetics_type::ResultTypeOfBinary;
use common_datavalues2::prelude::*;
use common_datavalues2::with_match_date_type_error;
use common_datavalues2::with_match_primitive_type;
use common_exception::ErrorCode;
use common_exception::Result;
use num::traits::AsPrimitive;
use num_traits::WrappingAdd;

use crate::scalars::cast_with_type;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Arithmetic2Description;
use crate::scalars::Function2;
use crate::scalars::Function2Factory;
use crate::scalars::Monotonicity;
use crate::scalars::ScalarBinaryExpression;
use crate::scalars::ScalarBinaryFunction;
use crate::scalars::DEFAULT_CAST_OPTIONS;

#[derive(Clone)]
pub struct ArithmeticFunction2;

impl ArithmeticFunction2 {
    pub fn register(factory: &mut Function2Factory) {
        factory.register_arithmetic("+", ArithmeticPlusFunction2::desc());
        factory.register_arithmetic("plus", ArithmeticPlusFunction2::desc());
    }
}

#[derive(Clone)]
pub struct BinaryArithmeticFunction2<L: Scalar, R: Scalar, O: Scalar, F> {
    op: DataValueBinaryOperator,
    result_type: DataTypePtr,
    binary: ScalarBinaryExpression<L, R, O, F>,
}

impl<L, R, O, F> BinaryArithmeticFunction2<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone + 'static,
{
    pub fn try_create_func(
        op: DataValueBinaryOperator,
        result_type: DataTypePtr,
        func: F,
    ) -> Result<Box<dyn Function2>> {
        let binary = ScalarBinaryExpression::<L, R, O, _>::new(func);
        Ok(Box::new(Self {
            op,
            result_type,
            binary,
        }))
    }
}

impl<L, R, O, F> Function2 for BinaryArithmeticFunction2<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone,
{
    fn name(&self) -> &str {
        "BinaryArithmeticFunction2"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(self.result_type.clone())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let col = self.binary.eval(columns[0].column(), columns[1].column())?;
        let col_type = col.data_type();
        let result: ColumnRef = Arc::new(col);
        if col_type != self.result_type {
            return cast_with_type(&result, &col_type, &self.result_type, &DEFAULT_CAST_OPTIONS);
        }
        Ok(result)
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        if args.len() != 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        match self.op {
            DataValueBinaryOperator::Plus => ArithmeticPlusFunction2::get_monotonicity(args),
            _ => Ok(Monotonicity::default()),
        }
    }
}

impl<L, R, O, F> fmt::Display for BinaryArithmeticFunction2<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}

#[derive(Clone, Debug, Default)]
struct AddFunction {}

impl<L, R, O> ScalarBinaryFunction<L, R, O> for AddFunction
where
    L: PrimitiveType + AsPrimitive<O>,
    R: PrimitiveType + AsPrimitive<O>,
    O: PrimitiveType + Add<Output = O>,
{
    fn eval(&self, l: L::RefType<'_>, r: R::RefType<'_>) -> O {
        l.to_owned_scalar().as_() + r.to_owned_scalar().as_()
    }
}

#[derive(Clone, Debug, Default)]
struct WrappingAddFunction {}

impl<L, R, O> ScalarBinaryFunction<L, R, O> for WrappingAddFunction
where
    L: PrimitiveType + AsPrimitive<O>,
    R: PrimitiveType + AsPrimitive<O>,
    O: IntegerType + WrappingAdd<Output = O>,
{
    fn eval(&self, l: L::RefType<'_>, r: R::RefType<'_>) -> O {
        l.to_owned_scalar()
            .as_()
            .wrapping_add(&r.to_owned_scalar().as_())
    }
}

pub struct ArithmeticPlusFunction2;

impl ArithmeticPlusFunction2 {
    pub fn try_create_func(
        _display_name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function2>> {
        let op = DataValueBinaryOperator::Plus;
        let left_type = args[0].data_type_id();
        let right_type = args[1].data_type_id();

        let error_fn = || -> Result<Box<dyn Function2>> {
            Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
                left_type, op, right_type
            )))
        };

        if left_type.is_interval() || right_type.is_interval() {
            todo!()
        }

        // Only support one of argument types is date type.
        if left_type.is_date_or_date_time() {
            return with_match_date_type_error!(left_type, |$T| {
                with_match_primitive_type!(right_type, |$D| {
                    BinaryArithmeticFunction2::<$T, $D, $T, _>::try_create_func(
                        op,
                        args[0].clone(),
                        AddFunction::default(),
                    )
                },{
                    error_fn()
                })
            });
        }

        if right_type.is_date_or_date_time() {
            return with_match_primitive_type!(left_type, |$T| {
                with_match_date_type_error!(right_type, |$D| {
                    BinaryArithmeticFunction2::<$T, $D, $D, _>::try_create_func(
                        op,
                        args[1].clone(),
                        AddFunction::default(),
                    )
                })
            },{
                error_fn()
            });
        }

        if !left_type.is_numeric() || !right_type.is_numeric() {
            return error_fn();
        }

        with_match_primitive_type!(left_type, |$T| {
            with_match_primitive_type!(right_type, |$D| {
                let result_type = <($T, $D) as ResultTypeOfBinary>::AddMul::to_data_type();
                match result_type.data_type_id() {
                    TypeID::UInt64 => BinaryArithmeticFunction2::<$T, $D, u64, _>::try_create_func(
                        op,
                        result_type,
                        WrappingAddFunction::default(),
                    ),
                    TypeID::Int64 => BinaryArithmeticFunction2::<$T, $D, i64, _>::try_create_func(
                        op,
                        result_type,
                        WrappingAddFunction::default(),
                    ),
                    _ => BinaryArithmeticFunction2::<$T, $D, <($T, $D) as ResultTypeOfBinary>::AddMul, _>::try_create_func(
                        op,
                        result_type,
                        AddFunction::default(),
                    ),
                }
            }, {
                error_fn()
            })
        }, {
            error_fn()
        })
    }

    pub fn desc() -> Arithmetic2Description {
        Arithmetic2Description::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(2),
        )
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        // For expression f(x) + g(x), only when both f(x) and g(x) are monotonic and have
        // same 'is_positive' can we get a monotonic expression.
        let f_x = &args[0];
        let g_x = &args[1];

        // if either one is non-monotonic, return non-monotonic
        if !f_x.is_monotonic || !g_x.is_monotonic {
            return Ok(Monotonicity::default());
        }

        // if f(x) is a constant value, return the monotonicity of g(x)
        if f_x.is_constant {
            return Ok(Monotonicity::create(
                g_x.is_monotonic,
                g_x.is_positive,
                g_x.is_constant,
            ));
        }

        // if g(x) is a constant value, return the monotonicity of f(x)
        if g_x.is_constant {
            return Ok(Monotonicity::create(
                f_x.is_monotonic,
                f_x.is_positive,
                f_x.is_constant,
            ));
        }

        // Now we have f(x) and g(x) both are non-constant.
        // When both are monotonic, but have different 'is_positive', we can't determine the monotonicity
        if f_x.is_positive != g_x.is_positive {
            return Ok(Monotonicity::default());
        }

        Ok(Monotonicity::create(true, f_x.is_positive, false))
    }
}
