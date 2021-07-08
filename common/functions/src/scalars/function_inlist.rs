// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::numerical_coercion;
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

macro_rules! make_contains {
    ($ARRAY:expr, $LIST_VALUES:expr, $NEGATED:expr, $SCALAR_VALUE:ident, $ARRAY_TYPE:ident) => {{
        let mut contains_null = false;
        let values = $LIST_VALUES
            .iter()
            .flat_map(|scalar| match scalar {
                DataValue::$SCALAR_VALUE(Some(v)) => Some(v.clone()),
                DataValue::$SCALAR_VALUE(None) => {
                    contains_null = true;
                    None
                }
                DataValue::Utf8(None) => {
                    contains_null = true;
                    None
                }
                datatype => {
                    unimplemented!("Unexpected type {} for InList", datatype)
                }
            })
            .collect::<Vec<_>>();

        let size = $ARRAY.len();
        let mut res = Vec::new();
        for i in 0..size {
            let x = $ARRAY.try_get(i)?;
            let val;
            if let DataValue::$SCALAR_VALUE(Some(v)) = x {
                val = v;
            } else {
                return Err(ErrorCode::LogicalError(format!(
                    "Get unexpected type in InList"
                )));
            }
            let contains = values.contains(&val);
            let b = match contains {
                true => !$NEGATED,
                false => {
                    if contains_null {
                        false
                    } else {
                        $NEGATED
                    }
                }
            };
            res.push(b);
        }
        Ok(DataColumn::Array(Series::new(res)))
    }};
}

#[derive(Clone, Debug)]
pub struct InListFunction {
    list: Vec<DataValue>,
    negated: bool,
    value_data_type: DataType,
}

impl InListFunction {
    pub fn try_create(
        list: Vec<DataValue>,
        negated: bool,
        value_data_type: DataType,
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(InListFunction {
            list,
            negated,
            value_data_type,
        }))
    }
}

impl Function for InListFunction {
    fn name(&self) -> &str {
        "InListFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(self.list[0].is_null())
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        let input_column = columns[0].clone();
        let array = match input_column {
            DataColumn::Array(array) => array,
            DataColumn::Constant(scalar, _size) => scalar.to_array()?,
        };

        let col_dtype = array.data_type();
        let array_cast = if col_dtype != self.value_data_type {
            let dtype = numerical_coercion(&col_dtype, &self.value_data_type)?;
            array.cast_with_type(&dtype)?
        } else {
            array
        };

        match &self.value_data_type {
            DataType::Float32 => {
                make_contains!(array_cast, self.list, self.negated, Float32, Float32Array)
            }
            DataType::Float64 => {
                make_contains!(array_cast, self.list, self.negated, Float64, Float64Array)
            }
            DataType::Int16 => {
                make_contains!(array_cast, self.list, self.negated, Int16, Int16Array)
            }
            DataType::Int32 => {
                make_contains!(array_cast, self.list, self.negated, Int32, Int32Array)
            }
            DataType::Int64 => {
                make_contains!(array_cast, self.list, self.negated, Int64, Int64Array)
            }
            DataType::Int8 => {
                make_contains!(array_cast, self.list, self.negated, Int8, Int8Array)
            }
            DataType::UInt16 => {
                make_contains!(array_cast, self.list, self.negated, UInt16, UInt16Array)
            }
            DataType::UInt32 => {
                make_contains!(array_cast, self.list, self.negated, UInt32, UInt32Array)
            }
            DataType::UInt64 => {
                make_contains!(array_cast, self.list, self.negated, UInt64, UInt64Array)
            }
            DataType::UInt8 => {
                make_contains!(array_cast, self.list, self.negated, UInt8, UInt8Array)
            }
            DataType::Boolean => {
                make_contains!(array_cast, self.list, self.negated, Boolean, BooleanArray)
            }
            datatype => {
                unimplemented!("InList does not support datatype {:?}.", datatype)
            }
        }
    }

    fn num_arguments(&self) -> usize {
        0
    }
}

impl fmt::Display for InListFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self.negated {
            true => "NoT",
            false => "",
        };
        write!(f, "{} IN {:?}", s, self.list)
    }
}
