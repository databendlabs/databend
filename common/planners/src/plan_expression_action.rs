// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::scalars::CastFunction;
use common_functions::scalars::Function;
use common_functions::scalars::FunctionFactory;

#[derive(Debug, Clone)]
pub enum ExpressionAction {
    /// Column which must be in input.
    Input(ActionInput),
    /// Constant column with known value.
    Constant(ActionConstant),
    Alias(ActionAlias),
    Function(ActionFunction),
    Exists(ActionExists),
}

#[derive(Debug, Clone)]
pub struct ActionInput {
    pub name: String,
    pub return_type: DataType,
}

#[derive(Debug, Clone)]
pub struct ActionConstant {
    pub name: String,
    pub value: DataValue,
}

#[derive(Debug, Clone)]
pub struct ActionAlias {
    pub name: String,
    pub arg_name: String,
    pub arg_type: DataType,
}

#[derive(Debug, Clone)]
pub struct ActionFunction {
    pub name: String,
    pub func_name: String,
    pub return_type: DataType,
    pub is_aggregated: bool,

    // for functions
    pub arg_names: Vec<String>,
    pub arg_types: Vec<DataType>,

    // only for aggregate functions
    pub arg_fields: Vec<DataField>,
}

#[derive(Debug, Clone)]
pub struct ActionExists {
    pub name: String,
}

impl ExpressionAction {
    pub fn column_name(&self) -> &str {
        match self {
            ExpressionAction::Input(input) => &input.name,
            ExpressionAction::Constant(c) => &c.name,
            ExpressionAction::Alias(a) => &a.name,
            ExpressionAction::Function(f) => &f.name,
            ExpressionAction::Exists(f) => &f.name,
        }
    }
}

impl ActionFunction {
    pub fn to_function(&self) -> Result<Box<dyn Function>> {
        if self.is_aggregated {
            return Err(ErrorCode::LogicalError(
                "Action must be non-aggregated function",
            ));
        }

        match self.func_name.as_str() {
            "cast" => Ok(CastFunction::create(self.return_type.clone())),
            _ => FunctionFactory::get(&self.func_name),
        }
    }

    pub fn to_aggregate_function(&self) -> Result<AggregateFunctionRef> {
        if !self.is_aggregated {
            return Err(ErrorCode::LogicalError(
                "Action must be aggregated function",
            ));
        }
        AggregateFunctionFactory::get(&self.func_name, self.arg_fields.clone())
    }
}
