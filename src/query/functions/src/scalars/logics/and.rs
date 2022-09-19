// Copyright 2022 Datafuse Labs.
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

use core::fmt;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::logic::LogicExpression;
use super::logic::LogicFunctionImpl;
use super::logic::LogicOperator;
use crate::calcute;
use crate::impl_logic_expression;
use crate::scalars::cast_column_field;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

impl_logic_expression!(LogicAndExpression, &, |lhs: bool, rhs: bool, lhs_v: bool, rhs_v: bool| -> (bool, bool) {
    (lhs & rhs,  (lhs_v & rhs_v) | (!lhs & lhs_v) | (!rhs & rhs_v))
});

/// Logical functions AND, OR, XOR and NOT support three-valued (or ternary) logic
/// https://en.wikibooks.org/wiki/Structured_Query_Language/NULLs_and_the_Three_Valued_Logic
///
/// Functions XOR and NOT rely on "default implementation for NULLs":
///   - if any of the arguments is of Nullable type, the return value type is Nullable
///   - if any of the arguments is NULL, the return value is NULL
///
/// Functions AND and OR provide their own special implementations for ternary logic
#[derive(Clone)]
pub struct LogicAndFunction;

impl LogicAndFunction {
    pub fn try_create(_display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        LogicFunctionImpl::<LogicAndExpression>::try_create(LogicOperator::And, args)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .disable_passthrough_null()
                .num_arguments(2),
        )
    }
}

#[derive(Clone)]
pub struct LogicAndFiltersFunction;

impl Function for LogicAndFiltersFunction {
    fn name(&self) -> &str {
        "and_filters"
    }

    fn return_type(&self) -> DataTypeImpl {
        bool::to_data_type()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        if columns.len() == 1 {
            return Ok(columns[1].column().clone());
        }

        let mut validity = None;
        for c in columns.iter() {
            let c = c.column();
            if c.is_const() {
                let v = c.get_bool(0)?;
                if !v {
                    let validity = MutableBitmap::from_len_zeroed(input_rows).into();
                    return Ok(BooleanColumn::from_arrow_data(validity).arc());
                }
            } else {
                let bools: &BooleanColumn = Series::check_get(c)?;
                match validity.as_mut() {
                    Some(v) => {
                        *v = &*v & bools.values();
                    }
                    None => validity = Some(bools.values().clone()),
                }
            }
        }
        let validity = validity.unwrap_or_else(|| MutableBitmap::from_len_set(input_rows).into());
        Ok(BooleanColumn::from_arrow_data(validity).arc())
    }
}

impl LogicAndFiltersFunction {
    pub fn try_create(_display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        if args
            .iter()
            .any(|arg| !matches!(arg, DataTypeImpl::Boolean(_)))
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Illegal type {:?} of argument of function and_filters, expect to be all boolean types",
                args
            )));
        }

        Ok(Box::new(Self))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, std::usize::MAX),
        )
    }
}

impl fmt::Display for LogicAndFiltersFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "and_filters")
    }
}
