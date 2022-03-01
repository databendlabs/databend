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
use std::sync::Arc;

use common_arrow::arrow::compute::comparison;
use common_arrow::arrow::compute::comparison::Simd8;
use common_arrow::arrow::compute::comparison::Simd8PartialEq;
use common_arrow::arrow::compute::comparison::Simd8PartialOrd;
use common_datavalues::prelude::*;
use common_datavalues::type_coercion::compare_coercion;
use common_datavalues::with_match_physical_primitive_type;
use common_datavalues::with_match_physical_primitive_type_error;
use common_datavalues::with_match_primitive_types_error;
use common_exception::ErrorCode;
use common_exception::Result;
use num::traits::AsPrimitive;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticCreator;
use crate::scalars::ArithmeticDescription;
use crate::scalars::ComparisonEqFunction;
use crate::scalars::ComparisonGtEqFunction;
use crate::scalars::ComparisonGtFunction;
use crate::scalars::ComparisonLikeFunction;
use crate::scalars::ComparisonLtEqFunction;
use crate::scalars::ComparisonLtFunction;
use crate::scalars::ComparisonNotEqFunction;
use crate::scalars::ComparisonRegexpFunction;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionFactory;
use crate::scalars::ScalarBinaryExpression;
use crate::scalars::ScalarBinaryFunction;

#[derive(Clone)]
pub struct ComparisonFunction {
    op: DataValueComparisonOperator,
}

impl ComparisonFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register_arithmetic(
            "=",
            EqualFunction::desc(DataValueComparisonOperator::Eq, "<>"),
        );
        factory.register("<", ComparisonLtFunction::desc());
        factory.register(">", ComparisonGtFunction::desc());
        factory.register("<=", ComparisonLtEqFunction::desc());
        factory.register(">=", ComparisonGtEqFunction::desc());
        factory.register("!=", ComparisonNotEqFunction::desc());
        factory.register("<>", ComparisonNotEqFunction::desc());
        factory.register("like", ComparisonLikeFunction::desc_like());
        factory.register("not like", ComparisonLikeFunction::desc_unlike());
        factory.register("regexp", ComparisonRegexpFunction::desc_regexp());
        factory.register("not regexp", ComparisonRegexpFunction::desc_unregexp());
        factory.register("rlike", ComparisonRegexpFunction::desc_regexp());
        factory.register("not rlike", ComparisonRegexpFunction::desc_unregexp());
    }

    pub fn try_create_func(op: DataValueComparisonOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(ComparisonFunction { op }))
    }
}

pub trait ComparisonImpl {
    fn eval_primitive<L, R, M>(
        _l: L::RefType<'_>,
        _r: R::RefType<'_>,
        _ctx: &mut EvalContext,
    ) -> bool
    where
        L: PrimitiveType + AsPrimitive<M>,
        R: PrimitiveType + AsPrimitive<M>,
        M: PrimitiveType + Simd8,
        M::Simd: Simd8PartialEq + Simd8PartialOrd,
    {
        unimplemented!()
    }

    fn eval_binary(_l: &[u8], _r: &[u8], _ctx: &mut EvalContext) -> bool {
        unimplemented!()
    }

    fn eval_bool(_l: bool, _r: bool, _ctx: &mut EvalContext) -> bool {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct EqualImpl;

impl ComparisonImpl for EqualImpl {
    fn eval_primitive<L, R, M>(l: L::RefType<'_>, r: R::RefType<'_>, _ctx: &mut EvalContext) -> bool
    where
        L: PrimitiveType + AsPrimitive<M>,
        R: PrimitiveType + AsPrimitive<M>,
        M: PrimitiveType + Simd8,
        M::Simd: Simd8PartialEq + Simd8PartialOrd,
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

pub type EqualFunction = ComparisonFunctionCreator<EqualImpl>;

pub struct ComparisonFunctionCreator<T> {
    t: PhantomData<T>,
}

impl<T> ComparisonFunctionCreator<T>
where T: ComparisonImpl + Send + Sync + Clone + 'static
{
    pub fn try_create_func(
        op: DataValueComparisonOperator,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        // expect array & struct
        let has_array_struct = args
            .iter()
            .any(|arg| matches!(arg.data_type_id(), TypeID::Struct | TypeID::Array));

        if has_array_struct {
            return Err(ErrorCode::BadArguments(format!(
                "Illegal types {:?} of argument of function {}, can not be struct or array",
                args, op
            )));
        }

        let lhs_id = args[0].data_type_id();
        let rhs_id = args[1].data_type_id();

        if args[0].eq(args[1]) {
            return with_match_physical_primitive_type!(lhs_id.to_physical_type(), |$T| {
                ComparisonFunction2::<$T, $T, bool, _>::try_create_func(
                    op,
                    args[0].clone(),
                    false,
                    T::eval_primitive::<$T, $T, $T>,
                )
            }, {
                match lhs_id {
                    TypeID::Boolean => ComparisonFunction2::<bool, bool, bool, _>::try_create_func(
                        op,
                        args[0].clone(),
                        false,
                        T::eval_bool,
                    ),
                    TypeID::String => ComparisonFunction2::<Vu8, Vu8, bool, _>::try_create_func(
                        op,
                        args[0].clone(),
                        false,
                        T::eval_binary,
                    ),
                    _ => Err(ErrorCode::IllegalDataType(format!(
                        "Can not compare {:?} with {:?}",
                        args[0], args[1]
                    ))),
                }
            });
        }

        if lhs_id.is_numeric() && rhs_id.is_numeric() {
            return with_match_primitive_types_error!(lhs_id, |$T| {
                with_match_primitive_types_error!(rhs_id, |$D| {
                    let least_supertype = <($T, $D) as ResultTypeOfBinary>::LeastSuper::to_data_type();
                    ComparisonFunction2::<$T, $D, bool, _>::try_create_func(
                            op,
                            least_supertype,
                            false,
                            T::eval_primitive::<$T, $D, <($T, $D) as ResultTypeOfBinary>::LeastSuper>,
                    )
                })
            });
        }

        let least_supertype = compare_coercion(args[0], args[1])?;
        with_match_physical_primitive_type_error!(least_supertype.data_type_id().to_physical_type(), |$T| {
            ComparisonFunction2::<$T, $T, bool, _>::try_create_func(
                op,
                least_supertype,
                true,
                T::eval_primitive::<$T, $T, $T>,
            )
        })
    }

    pub fn desc(op: DataValueComparisonOperator, negative_name: &str) -> ArithmeticDescription {
        let function_creator: ArithmeticCreator =
            Box::new(move |display_name, args| Self::try_create_func(op.clone(), args));

        ArithmeticDescription::creator(Box::new(function_creator)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function(negative_name)
                .bool_function()
                .num_arguments(2),
        )
    }
}

#[derive(Clone)]
pub struct ComparisonFunction2<L: Scalar, R: Scalar, O: Scalar, F> {
    op: DataValueComparisonOperator,
    least_supertype: DataTypePtr,
    need_cast: bool,
    binary: ScalarBinaryExpression<L, R, O, F>,
}

impl<L, R, O, F> ComparisonFunction2<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone + 'static,
{
    pub fn try_create_func(
        op: DataValueComparisonOperator,
        least_supertype: DataTypePtr,
        need_cast: bool,
        func: F,
    ) -> Result<Box<dyn Function>> {
        let binary = ScalarBinaryExpression::<L, R, O, _>::new(func);
        Ok(Box::new(Self {
            op,
            least_supertype,
            need_cast,
            binary,
        }))
    }
}

impl<L, R, O, F> Function for ComparisonFunction2<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone,
{
    fn name(&self) -> &str {
        "ComparisonFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(BooleanType::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let lhs = if self.need_cast && columns[0].data_type() != &self.least_supertype {
            cast_column_field(&columns[0], &self.least_supertype)?
        } else {
            columns[0].column().clone()
        };

        let rhs = if self.need_cast && columns[1].data_type() != &self.least_supertype {
            cast_column_field(&columns[1], &self.least_supertype)?
        } else {
            columns[1].column().clone()
        };

        let col = self.binary.eval(&lhs, &rhs, &mut EvalContext::default())?;
        Ok(Arc::new(col))
    }
}

impl<L, R, O, F> fmt::Display for ComparisonFunction2<L, R, O, F>
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

impl Function for ComparisonFunction {
    fn name(&self) -> &str {
        "ComparisonFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        // expect array & struct
        let has_array_struct = args
            .iter()
            .any(|arg| matches!(arg.data_type_id(), TypeID::Struct | TypeID::Array));

        if has_array_struct {
            return Err(ErrorCode::BadArguments(format!(
                "Illegal types {:?} of argument of function {}, can not be struct or array",
                args,
                self.name()
            )));
        }

        Ok(BooleanType::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        if columns[0].data_type() != columns[1].data_type() {
            // TODO cached it inside the function
            let least_supertype = compare_coercion(columns[0].data_type(), columns[1].data_type())?;
            let col0 = cast_column_field(&columns[0], &least_supertype)?;
            let col1 = cast_column_field(&columns[1], &least_supertype)?;

            let f0 = DataField::new(columns[0].field().name(), least_supertype.clone());
            let f1 = DataField::new(columns[1].field().name(), least_supertype.clone());

            let columns = vec![
                ColumnWithField::new(col0, f0),
                ColumnWithField::new(col1, f1),
            ];
            return self.eval(&columns, input_rows);
        }

        // TODO, this already convert to full column
        // Better to use comparator with scalar of arrow2

        let array0 = columns[0].column().as_arrow_array();
        let array1 = columns[1].column().as_arrow_array();

        let f = match self.op {
            DataValueComparisonOperator::Eq => comparison::eq,
            DataValueComparisonOperator::Lt => comparison::lt,
            DataValueComparisonOperator::LtEq => comparison::lt_eq,
            DataValueComparisonOperator::Gt => comparison::gt,
            DataValueComparisonOperator::GtEq => comparison::gt_eq,
            DataValueComparisonOperator::NotEq => comparison::neq,
            _ => unreachable!(),
        };

        let result = f(array0.as_ref(), array1.as_ref());
        let result = BooleanColumn::new(result);
        Ok(Arc::new(result))
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
