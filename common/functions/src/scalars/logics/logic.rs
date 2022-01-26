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

use common_datavalues2::BooleanType;
use common_datavalues2::ColumnBuilder;
use common_datavalues2::ColumnRef;
use common_datavalues2::ColumnViewer;
use common_datavalues2::ColumnsWithField;
use common_datavalues2::DataTypePtr;
use common_datavalues2::NullableColumnBuilder;
use common_datavalues2::NullableType;
use common_exception::ErrorCode;
use common_exception::Result;

use super::xor::LogicXorFunction;
use super::LogicAndFunction;
use super::LogicNotFunction;
use super::LogicOrFunction;
use crate::scalars::cast_column_field;
use crate::scalars::Function2;
use crate::scalars::Function2Factory;

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
    pub fn try_create(op: LogicOperator) -> Result<Box<dyn Function2>> {
        Ok(Box::new(Self { op }))
    }

    pub fn register(factory: &mut Function2Factory) {
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

        if nullable {
            let col_viewer = ColumnViewer::<bool>::create(&col)?;

            let mut builder = NullableColumnBuilder::<bool>::with_capacity(input_rows);

            for idx in 0..input_rows {
                builder.append(!col_viewer.value(idx), col_viewer.valid_at(idx));
            }

            Ok(builder.build(input_rows))
        } else {
            let col_viewer = ColumnViewer::<bool>::create(&col)?;
            let mut builder = ColumnBuilder::<bool>::with_capacity(input_rows);

            for idx in 0..input_rows {
                builder.append(!col_viewer.value(idx));
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
            let lhs_viewer = ColumnViewer::<bool>::create(&lhs)?;
            let rhs_viewer = ColumnViewer::<bool>::create(&rhs)?;

            let mut builder = NullableColumnBuilder::<bool>::with_capacity(input_rows);

            let func = match self.op {
                LogicOperator::And => LogicAndFunction::eval_with_null,
                LogicOperator::Or => LogicOrFunction::eval_with_null,
                LogicOperator::Xor => LogicXorFunction::eval_with_null,
                LogicOperator::Not => return Err(ErrorCode::LogicalError("never happen")),
            };

            for idx in 0..input_rows {
                let (val, valid) = func(
                    lhs_viewer.value(idx),
                    rhs_viewer.value(idx),
                    lhs_viewer.valid_at(idx),
                    rhs_viewer.valid_at(idx),
                );
                builder.append(val, valid);
            }

            Ok(builder.build(input_rows))
        } else {
            let lhs_viewer = ColumnViewer::<bool>::create(&lhs)?;
            let rhs_viewer = ColumnViewer::<bool>::create(&rhs)?;

            let mut builder = ColumnBuilder::<bool>::with_capacity(input_rows);

            let func = match self.op {
                LogicOperator::And => LogicAndFunction::eval,
                LogicOperator::Or => LogicOrFunction::eval,
                LogicOperator::Xor => LogicXorFunction::eval,
                LogicOperator::Not => return Err(ErrorCode::LogicalError("never happen")),
            };

            for idx in 0..input_rows {
                builder.append(func(lhs_viewer.value(idx), rhs_viewer.value(idx)));
            }

            Ok(builder.build(input_rows))
        }
    }
}

impl Function2 for LogicFunction {
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
