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

use std::f64::consts::E;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::ScalarBinaryExpression;
use crate::scalars::ScalarUnaryExpression;

/// Const f64 is now allowed.
/// feature(adt_const_params) is not stable & complete
pub trait Base: Send + Sync + Clone + 'static {
    fn base() -> f64;
}

#[derive(Clone)]
pub struct EBase;

#[derive(Clone)]
pub struct TenBase;

#[derive(Clone)]
pub struct TwoBase;

impl Base for EBase {
    fn base() -> f64 {
        E
    }
}

impl Base for TenBase {
    fn base() -> f64 {
        10f64
    }
}

impl Base for TwoBase {
    fn base() -> f64 {
        2f64
    }
}
#[derive(Clone)]
pub struct GenericLogFunction<T> {
    display_name: String,
    t: PhantomData<T>,
}

impl<T: Base> GenericLogFunction<T> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            t: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 2),
        )
    }

    fn log<S>(value: S, _ctx: &mut EvalContext) -> f64
    where S: AsPrimitive<f64> {
        value.as_().log(T::base())
    }

    fn log_with_base<S, B>(base: S, value: B, _ctx: &mut EvalContext) -> f64
    where
        S: AsPrimitive<f64>,
        B: AsPrimitive<f64>,
    {
        value.as_().log(base.as_())
    }
}

impl<T: Base> Function for GenericLogFunction<T> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        for arg in args {
            assert_numeric(*arg)?;
        }
        Ok(f64::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let mut ctx = EvalContext::default();
        if columns.len() == 1 {
            with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
                let unary = ScalarUnaryExpression::<$S, f64, _>::new(Self::log);
                let col = unary.eval(columns[0].column(), &mut ctx)?;
                Ok(Arc::new(col))
            },{
                unreachable!()
            })
        } else {
            with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
                with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$T| {
                    let binary = ScalarBinaryExpression::<$S, $T, f64, _>::new(Self::log_with_base);
                    let col = binary.eval(columns[0].column(), columns[1].column(), &mut ctx)?;
                    Ok(Arc::new(col))
                },{
                    unreachable!()
                })
            },{
                unreachable!()
            })
        }
    }
}

impl<T: Base> fmt::Display for GenericLogFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}

pub type LnFunction = GenericLogFunction<EBase>;
pub type LogFunction = GenericLogFunction<EBase>;
pub type Log10Function = GenericLogFunction<TenBase>;
pub type Log2Function = GenericLogFunction<TwoBase>;
