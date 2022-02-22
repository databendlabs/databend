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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::xor::LogicXorFunction;
use super::LogicAndFunction;
use super::LogicNotFunction;
use super::LogicOrFunction;
use crate::scalars::cast_column_field;
use crate::scalars::Function;
use crate::scalars::FunctionFactory;

#[derive(Clone)]
pub struct LogicFunction {
    op: LogicOperator,
}

#[derive(Clone, Debug)]
pub enum LogicOperator {
    Not,
    And,
    Or,
    Xor,
}

impl LogicFunction {
    pub fn try_create(op: LogicOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self { op }))
    }

    pub fn register(factory: &mut FunctionFactory) {
        factory.register("and", LogicAndFunction::desc());
        factory.register("or", LogicOrFunction::desc());
        factory.register("not", LogicNotFunction::desc());
        factory.register("xor", LogicXorFunction::desc());
    }

    fn eval_not(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let mut nullable = false;
        if columns[0].data_type().is_nullable() {
            nullable = true;
        }

        let dt = if nullable {
            Arc::new(NullableType::create(BooleanType::arc()))
        } else {
            BooleanType::arc()
        };

        let col = cast_column_field(&columns[0], &dt)?;

        let col_viewer = bool::try_create_viewer(&col)?;

        if nullable {
            let mut builder = NullableColumnBuilder::<bool>::with_capacity(input_rows);

            for (idx, data) in col_viewer.iter().enumerate() {
                builder.append(!data, col_viewer.valid_at(idx));
            }

            Ok(builder.build(input_rows))
        } else {
            let mut builder = ColumnBuilder::<bool>::with_capacity(input_rows);

            for value in col_viewer.iter() {
                builder.append(!value);
            }
            Ok(builder.build(input_rows))
        }
    }

    fn eval_and_not_or(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let mut nullable = false;
        if columns[0].data_type().is_nullable() || columns[1].data_type().is_nullable() {
            nullable = true;
        }

        let dt = if nullable {
            Arc::new(NullableType::create(BooleanType::arc()))
        } else {
            BooleanType::arc()
        };

        let lhs = cast_column_field(&columns[0], &dt)?;
        let rhs = cast_column_field(&columns[1], &dt)?;

        if nullable {
            let lhs_viewer = bool::try_create_viewer(&lhs)?;
            let rhs_viewer = bool::try_create_viewer(&rhs)?;

            let lhs_viewer_iter = lhs_viewer.iter();
            let rhs_viewer_iter = rhs_viewer.iter();

            let mut builder = NullableColumnBuilder::<bool>::with_capacity(input_rows);

            macro_rules! calcute_with_null {
                ($lhs_viewer: expr, $rhs_viewer: expr,  $lhs_viewer_iter: expr, $rhs_viewer_iter: expr, $builder: expr, $func: expr) => {
                    for (a, (idx, b)) in $lhs_viewer_iter.zip($rhs_viewer_iter.enumerate()) {
                        let (val, valid) =
                            $func(a, b, $lhs_viewer.valid_at(idx), $rhs_viewer.valid_at(idx));
                        $builder.append(val, valid);
                    }
                };
            }

            match self.op {
                LogicOperator::And => calcute_with_null!(
                    lhs_viewer,
                    rhs_viewer,
                    lhs_viewer_iter,
                    rhs_viewer_iter,
                    builder,
                    |lhs: bool, rhs: bool, l_valid: bool, r_valid: bool| -> (bool, bool) {
                        (lhs & rhs, l_valid & r_valid)
                    }
                ),
                LogicOperator::Or => calcute_with_null!(
                    lhs_viewer,
                    rhs_viewer,
                    lhs_viewer_iter,
                    rhs_viewer_iter,
                    builder,
                    |lhs: bool, rhs: bool, _l_valid: bool, _r_valid: bool| -> (bool, bool) {
                        (lhs || rhs, lhs || rhs)
                    }
                ),
                LogicOperator::Xor => calcute_with_null!(
                    lhs_viewer,
                    rhs_viewer,
                    lhs_viewer_iter,
                    rhs_viewer_iter,
                    builder,
                    |lhs: bool, rhs: bool, l_valid: bool, r_valid: bool| -> (bool, bool) {
                        (lhs ^ rhs, l_valid & r_valid)
                    }
                ),
                LogicOperator::Not => return Err(ErrorCode::LogicalError("never happen")),
            };

            Ok(builder.build(input_rows))
        } else {
            let lhs_viewer = bool::try_create_viewer(&lhs)?;
            let rhs_viewer = bool::try_create_viewer(&rhs)?;

            let mut builder = ColumnBuilder::<bool>::with_capacity(input_rows);

            macro_rules! calcute {
                ($lhs_viewer: expr, $rhs_viewer: expr, $builder: expr, $func: expr) => {
                    for (a, b) in ($lhs_viewer.iter().zip($rhs_viewer.iter())) {
                        $builder.append($func(a, b));
                    }
                };
            }

            match self.op {
                LogicOperator::And => calcute!(
                    lhs_viewer,
                    rhs_viewer,
                    builder,
                    |lhs: bool, rhs: bool| -> bool { lhs & rhs }
                ),
                LogicOperator::Or => calcute!(
                    lhs_viewer,
                    rhs_viewer,
                    builder,
                    |lhs: bool, rhs: bool| -> bool { lhs || rhs }
                ),
                LogicOperator::Xor => calcute!(
                    lhs_viewer,
                    rhs_viewer,
                    builder,
                    |lhs: bool, rhs: bool| -> bool { lhs ^ rhs }
                ),
                LogicOperator::Not => return Err(ErrorCode::LogicalError("never happen")),
            };

            Ok(builder.build(input_rows))
        }
    }
}

impl Function for LogicFunction {
    fn name(&self) -> &str {
        "LogicFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        match self.op {
            LogicOperator::Not => {
                if args[0].is_nullable() {
                    Ok(Arc::new(NullableType::create(BooleanType::arc())))
                } else {
                    Ok(BooleanType::arc())
                }
            }
            _ => {
                if args[0].is_nullable() || args[1].is_nullable() {
                    Ok(Arc::new(NullableType::create(BooleanType::arc())))
                } else {
                    Ok(BooleanType::arc())
                }
            }
        }
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        match self.op {
            LogicOperator::Not => self.eval_not(columns, input_rows),
            _ => self.eval_and_not_or(columns, input_rows),
        }
    }

    fn passthrough_null(&self) -> bool {
        !matches!(self.op, LogicOperator::Or)
    }
}

impl std::fmt::Display for LogicFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.op)
    }
}
