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

use std::fmt::Display;
use std::fmt::Formatter;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check;
use common_expression::types::DataType;
use common_expression::Expr;
use common_expression::Literal;
use common_expression::RawExpr;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;

type IndexType = usize;

/// Serializable and desugared representation of `Scalar`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PhysicalScalar {
    IndexedVariable {
        index: IndexType,
        data_type: DataType,

        display_name: String,
    },
    Constant {
        value: Literal,
        data_type: DataType,
    },
    Function {
        name: String,
        args: Vec<PhysicalScalar>,
        return_type: DataType,
    },

    Cast {
        input: Box<PhysicalScalar>,
        target: DataType,
    },
}

impl PhysicalScalar {
    pub fn data_type(&self) -> DataType {
        match self {
            PhysicalScalar::Constant { data_type, .. } => data_type.clone(),
            PhysicalScalar::Function { return_type, .. } => return_type.clone(),
            PhysicalScalar::Cast { target, .. } => target.clone(),
            PhysicalScalar::IndexedVariable { data_type, .. } => data_type.clone(),
        }
    }

    /// Display with readable variable name.
    pub fn pretty_display(&self) -> String {
        match self {
            PhysicalScalar::Constant { value, .. } => value.to_string(),
            PhysicalScalar::Function { name, args, .. } => {
                let args = args
                    .iter()
                    .map(|arg| arg.pretty_display())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name, args)
            }
            PhysicalScalar::Cast { input, target } => {
                format!("CAST({} AS {})", input.pretty_display(), target.sql_name(),)
            }
            PhysicalScalar::IndexedVariable { display_name, .. } => display_name.clone(),
        }
    }

    /// Convert to `RawExpr`
    pub fn as_raw_expr(&self) -> RawExpr {
        match self {
            PhysicalScalar::Constant { value, .. } => RawExpr::Literal {
                span: None,
                lit: value.clone(),
            },
            PhysicalScalar::Function { name, args, .. } => {
                let args = args.iter().map(|arg| arg.as_raw_expr()).collect::<Vec<_>>();
                RawExpr::FunctionCall {
                    span: None,
                    name: name.clone(),
                    args,
                    params: vec![],
                }
            }
            PhysicalScalar::Cast { input, target } => {
                let is_try = match target {
                    DataType::Nullable(_) => true,
                    _ => false,
                };
                RawExpr::Cast {
                    span: None,
                    is_try,
                    expr: Box::new(input.as_raw_expr()),
                    dest_type: target.clone(),
                }
            }
            PhysicalScalar::IndexedVariable {
                index, data_type, ..
            } => RawExpr::ColumnRef {
                span: None,
                id: *index,
                data_type: data_type.clone(),
            },
        }
    }

    /// Convert to `Expr` by type checking.
    pub fn as_expr(&self) -> Result<Expr> {
        let raw_expr = self.as_raw_expr();
        let registry = &BUILTIN_FUNCTIONS;
        let expr = check(&raw_expr, registry)
            .map_err(|(_, _e)| ErrorCode::Internal("Invalid expression"))?;
        Ok(expr)
    }
}

impl Display for PhysicalScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            PhysicalScalar::Constant { value, .. } => write!(f, "{}", value),
            PhysicalScalar::Function { name, args, .. } => write!(
                f,
                "{}({})",
                name,
                args.iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            PhysicalScalar::Cast { input, target } => {
                write!(f, "CAST({} AS {})", input, target.sql_name())
            }
            PhysicalScalar::IndexedVariable { index, .. } => write!(f, "${index}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionDesc {
    pub sig: AggregateFunctionSignature,
    pub output_column: IndexType,
    pub args: Vec<IndexType>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionSignature {
    pub name: String,
    pub args: Vec<DataType>,
    pub params: Vec<Literal>,
    pub return_type: DataType,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortDesc {
    pub asc: bool,
    pub nulls_first: bool,
    pub order_by: IndexType,
}
