// Copyright 2022 Datafuse Labs
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

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::Result;
use common_io::prelude::*;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::scalar_unary_op;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct GenericHumanizeFunction<T> {
    display_name: String,
    t: PhantomData<T>,
}

pub trait HumanizeConvertFunction: Send + Sync + Clone + 'static {
    fn convert(v: impl AsPrimitive<f64>, _ctx: &mut EvalContext) -> Vec<u8>;
}

impl<T> GenericHumanizeFunction<T>
where T: HumanizeConvertFunction
{
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        assert_numeric(args[0])?;
        Ok(Box::new(GenericHumanizeFunction::<T> {
            display_name: display_name.to_string(),
            t: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl<T> Function for GenericHumanizeFunction<T>
where T: HumanizeConvertFunction
{
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        Vu8::to_data_type()
    }

    fn eval(
        &self,
        _func_ctx: crate::scalars::FunctionContext,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$F| {
            let col = scalar_unary_op::<$F, Vu8, _>(columns[0].column(), T::convert, &mut EvalContext::default())?;
            Ok(col.arc())
        },{
            unreachable!()
        })
    }
}

impl<T> fmt::Display for GenericHumanizeFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[derive(Clone)]
pub struct HumanizeSizeConvertFunction;

impl HumanizeConvertFunction for HumanizeSizeConvertFunction {
    fn convert(v: impl AsPrimitive<f64>, _: &mut EvalContext) -> Vec<u8> {
        Vec::from(convert_byte_size(v.as_()))
    }
}

#[derive(Clone)]
pub struct HumanizeNumberConvertFunction;

impl HumanizeConvertFunction for HumanizeNumberConvertFunction {
    fn convert(v: impl AsPrimitive<f64>, _: &mut EvalContext) -> Vec<u8> {
        Vec::from(convert_number_size(v.as_()))
    }
}

pub type HumanizeSizeFunction = GenericHumanizeFunction<HumanizeSizeConvertFunction>;
pub type HumanizeNumberFunction = GenericHumanizeFunction<HumanizeNumberConvertFunction>;
