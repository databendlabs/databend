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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use common_datavalues::format_data_type_sql;
use common_datavalues::DataField;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;

/// Serializable and desugared representation of `Scalar`.
#[derive(Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expression {
    IndexedVariable {
        name: String,
        data_type: DataTypeImpl,
    },
    Constant {
        value: DataValue,
        data_type: DataTypeImpl,
    },
    Function {
        name: String,
        args: Vec<Expression>,
        return_type: DataTypeImpl,
    },

    Cast {
        input: Box<Expression>,
        target: DataTypeImpl,
    },
}

impl Expression {
    pub fn data_type(&self) -> DataTypeImpl {
        match self {
            Expression::Constant { data_type, .. } => data_type.clone(),
            Expression::Function { return_type, .. } => return_type.clone(),
            Expression::Cast { target, .. } => target.clone(),
            Expression::IndexedVariable { data_type, .. } => data_type.clone(),
        }
    }

    pub fn to_data_field(&self) -> DataField {
        let name = self.column_name();
        let data_type = self.data_type();
        DataField::new(&name, data_type)
    }

    /// Display with readable variable name.
    pub fn column_name(&self) -> String {
        match self {
            Expression::Constant { value, .. } => common_datavalues::format_datavalue_sql(value),
            Expression::Function { name, args, .. } => match name.as_str() {
                "+" | "-" | "*" | "/" | "%" | ">=" | "<=" | "=" | "!=" | ">" | "<" | "or"
                | "and"
                    if args.len() == 2 =>
                {
                    format!(
                        "({} {} {})",
                        args[0].column_name(),
                        name,
                        args[1].column_name()
                    )
                }
                _ => {
                    let args = args
                        .iter()
                        .map(|arg| arg.column_name())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("{}({})", name, args)
                }
            },
            Expression::Cast { input, target } => format!(
                "CAST({} AS {})",
                input.column_name(),
                format_data_type_sql(target)
            ),
            Expression::IndexedVariable { name, .. } => name.clone(),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Expression::Constant { value, .. } => write!(f, "{}", value),
            Expression::Function { name, args, .. } => write!(
                f,
                "{}({})",
                name,
                args.iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Expression::Cast { input, target } => {
                write!(f, "CAST({} AS {})", input, format_data_type_sql(target))
            }
            Expression::IndexedVariable { name, .. } => write!(f, "${name}"),
        }
    }
}

impl Debug for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Expression::Constant { value, .. } => write!(f, "{:#}", value),
            Expression::Function { name, args, .. } => match name.as_str() {
                "+" | "-" | "*" | "/" | "%" | ">=" | "<=" | "=" | "!=" | ">" | "<" | "or"
                | "and"
                    if args.len() == 2 =>
                {
                    write!(f, "({:?} {} {:?})", args[0], name, args[1])
                }
                _ => {
                    write!(f, "{}(", name)?;
                    for (i, _) in args.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{:?}", args[i])?;
                    }
                    write!(f, ")")
                }
            },
            Expression::Cast { input, target } => {
                write!(f, "cast({:?} as {})", input, format_data_type_sql(target))
            }
            Expression::IndexedVariable { name, .. } => write!(f, "{:#}", name),
        }
    }
}
