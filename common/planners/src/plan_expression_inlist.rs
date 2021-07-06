// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::Float32Array;
use common_arrow::arrow::array::Float64Array;
use common_arrow::arrow::array::Int16Array;
use common_arrow::arrow::array::Int32Array;
use common_arrow::arrow::array::Int64Array;
use common_arrow::arrow::array::Int8Array;
use common_arrow::arrow::array::StringOffsetSizeTrait;
use common_arrow::arrow::array::UInt16Array;
use common_arrow::arrow::array::UInt32Array;
use common_arrow::arrow::array::UInt64Array;
use common_arrow::arrow::array::UInt8Array;
use common_arrow::arrow::record_batch::RecordBatch;
use common_datavalues::columns::DataColumn;
//use common_datavalues::series::Series;
use common_datavalues::prelude::*;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::Expression;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct InListExpr {
    expr: Box<Expression>,
    list: Vec<Expression>,
    negated: bool,
}

impl InListExpr {
    /// Create a new InList expression
    pub fn new(expr: Box<Expression>, list: Vec<Expression>, negated: bool) -> Self {
        Self {
            expr,
            list,
            negated,
        }
    }

    /// Input expression
    pub fn expr(&self) -> &Box<Expression> {
        &self.expr
    }

    /// List to search in
    pub fn list(&self) -> &[Expression] {
        &self.list
    }

    /// Is this negated e.g. NOT IN LIST
    pub fn negated(&self) -> bool {
        self.negated
    }
}

macro_rules! make_contains {
    ($ARRAY:expr, $LIST_VALUES:expr, $NEGATED:expr, $SCALAR_VALUE:ident, $ARRAY_TYPE:ident) => {{
        //let array = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();

        let mut contains_null = false;
        let values = $LIST_VALUES
            .iter()
            .flat_map(|scalar| match scalar {
                DataValue::$SCALAR_VALUE(Some(v)) => Some(*v),
                DataValue::$SCALAR_VALUE(None) => {
                    contains_null = true;
                    None
                }
                DataValue::Utf8(None) => {
                    contains_null = true;
                    None
                }
                datatype => unimplemented!("Unexpected type {} for InList", datatype),
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
                true => {
                    if $NEGATED {
                        false
                    } else {
                        true
                    }
                }
                false => {
                    if contains_null {
                        false
                    } else if $NEGATED {
                        true
                    } else {
                        false
                    }
                }
            };
            res.push(b);
        }
        Ok(DataColumn::Array(Series::new(res)))
    }};
}

impl InListExpr {
    fn evaluate(
        &self,
        value: DataColumn,
        batch: &RecordBatch,
        input_schema: &DataSchemaRef,
    ) -> Result<DataColumn> {
        let value_data_type = self.expr.to_data_type(input_schema)?;
        let mut list_values = Vec::new();

        for e in self.list.iter() {
            if let Expression::Literal(l) = e {
                list_values.push(l);
            } else {
                unimplemented!("InList contains invalid datatype.")
            }
        }

        let array = match value {
            DataColumn::Array(array) => array,
            DataColumn::Constant(scalar, size) => scalar.to_array()?,
        };

        match value_data_type {
            DataType::Float32 => {
                make_contains!(array, list_values, self.negated, Float32, Float32Array)
            }
            DataType::Float64 => {
                make_contains!(array, list_values, self.negated, Float64, Float64Array)
            }
            DataType::Int16 => {
                make_contains!(array, list_values, self.negated, Int16, Int16Array)
            }
            DataType::Int32 => {
                make_contains!(array, list_values, self.negated, Int32, Int32Array)
            }
            DataType::Int64 => {
                make_contains!(array, list_values, self.negated, Int64, Int64Array)
            }
            DataType::Int8 => {
                make_contains!(array, list_values, self.negated, Int8, Int8Array)
            }
            DataType::UInt16 => {
                make_contains!(array, list_values, self.negated, UInt16, UInt16Array)
            }
            DataType::UInt32 => {
                make_contains!(array, list_values, self.negated, UInt32, UInt32Array)
            }
            DataType::UInt64 => {
                make_contains!(array, list_values, self.negated, UInt64, UInt64Array)
            }
            DataType::UInt8 => {
                make_contains!(array, list_values, self.negated, UInt8, UInt8Array)
            }
            DataType::Boolean => {
                make_contains!(array, list_values, self.negated, Boolean, BooleanArray)
            }
            //DataType::Utf8 => self.compare_utf8::<i32>(array, list_values, self.negated),
            datatype => {
                unimplemented!("InList does not support datatype {:?}.", datatype)
            }
        }
    }
}
