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

use common_datavalues2::prelude::*;
use common_datavalues2::with_match_primitive_type;
use common_exception::ErrorCode;
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticDescription;
use crate::scalars::Function2;

pub struct ArithmeticIntDivFunction;

impl ArithmeticIntDivFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function2>> {
        let left_type = remove_nullable(args[0]).data_type_id();
        let right_type = remove_nullable(args[1]).data_type_id();

        let error_fn = || -> Result<Box<dyn Function2>> {
            Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported arithmetic ({:?}) div ({:?})",
                left_type, right_type
            )))
        };

        with_match_primitive_type!(left_type, |$T| {
            with_match_primitive_type!(right_type, |$D| {
                Ok(Box::new(IntDivFunctionImpl::<$T, $D, <($T, $D) as ResultTypeOfBinary>::IntDiv>::default()))
            }, {
                error_fn()
            })
        }, {
            error_fn()
        })
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(2),
        )
    }
}

#[derive(Clone, Default)]
pub struct IntDivFunctionImpl<L, R, O> {
    l: PhantomData<L>,
    r: PhantomData<R>,
    o: PhantomData<O>,
}

impl<L, R, O> Function2 for IntDivFunctionImpl<L, R, O>
where
    f64: AsPrimitive<O>,
    L: PrimitiveType + AsPrimitive<f64>,
    R: PrimitiveType + AsPrimitive<f64>,
    O: IntegerType + ToDataType,
{
    fn name(&self) -> &str {
        "IntDivFunctionImpl"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(O::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let left = ColumnViewerIter::<L>::try_create(columns[0].column())?;
        let right = ColumnViewerIter::<R>::try_create(columns[1].column())?;
        let mut col_builder = MutablePrimitiveColumn::<O>::with_capacity(left.size);
        for (l, r) in left.zip(right) {
            let l = l.to_owned_scalar().as_();
            let r = r.to_owned_scalar().as_();
            if std::intrinsics::unlikely(r == 0.0) {
                return Err(ErrorCode::BadArguments("Division by zero"));
            }
            let o = AsPrimitive::<O>::as_(l / r);
            col_builder.append_value(o);
        }
        Ok(col_builder.to_column())
    }
}

impl<L, R, O> fmt::Display for IntDivFunctionImpl<L, R, O> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "div")
    }
}
