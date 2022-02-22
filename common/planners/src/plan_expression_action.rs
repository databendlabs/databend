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

use common_datavalues::prelude::*;
use common_functions::scalars::Function;

#[derive(Debug, Clone)]
pub enum ExpressionAction {
    /// Column which must be in input.
    Input(ActionInput),
    /// Constant column with known value.
    Constant(ActionConstant),
    Alias(ActionAlias),
    Function(ActionFunction),
}

#[derive(Debug, Clone)]
pub struct ActionInput {
    pub name: String,
    pub return_type: DataTypePtr,
}

#[derive(Debug, Clone)]
pub struct ActionConstant {
    pub name: String,
    pub value: DataValue,
    pub data_type: DataTypePtr,
}

#[derive(Debug, Clone)]
pub struct ActionAlias {
    pub name: String,
    pub arg_name: String,
    pub arg_type: DataTypePtr,
}

#[derive(Clone)]
pub struct ActionFunction {
    pub name: String,
    pub func_name: String,
    pub return_type: DataTypePtr,
    pub func: Box<dyn Function>,

    // for functions
    pub arg_names: Vec<String>,
    pub arg_types: Vec<DataTypePtr>,
}

impl ExpressionAction {
    pub fn column_name(&self) -> &str {
        match self {
            ExpressionAction::Input(input) => &input.name,
            ExpressionAction::Constant(c) => &c.name,
            ExpressionAction::Alias(a) => &a.name,
            ExpressionAction::Function(f) => &f.name,
        }
    }
}

impl fmt::Debug for ActionFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ActionFunction")
            .field("name", &self.name)
            .field("func_name", &self.func_name)
            .field("return_type", &self.return_type)
            .field("arg_names", &self.arg_names)
            .field("arg_types", &self.arg_types)
            .finish()
    }
}
