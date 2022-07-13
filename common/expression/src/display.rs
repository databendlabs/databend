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

use comfy_table::Cell;
use comfy_table::Table;
use itertools::Itertools;

use crate::chunk::Chunk;
use crate::expression::Expr;
use crate::expression::Literal;
use crate::expression::RawExpr;
use crate::function::Function;
use crate::property::FunctionProperty;
use crate::property::ValueProperty;
use crate::types::DataType;
use crate::types::ValueType;
use crate::values::ScalarRef;
use crate::values::Value;
use crate::values::ValueRef;

impl Debug for Chunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        table.set_header(vec!["Column ID", "Column Data"]);

        for (i, col) in self.columns().iter().enumerate() {
            table.add_row(vec![i.to_string(), format!("{:?}", col)]);
        }

        write!(f, "{}", table)
    }
}

impl Display for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        table.set_header((0..self.num_columns()).map(|idx| format!("Column {idx}")));

        for index in 0..self.num_rows() {
            let row: Vec<_> = self
                .columns()
                .iter()
                .map(|col| col.index(index).to_string())
                .map(Cell::new)
                .collect();
            table.add_row(row);
        }

        write!(f, "{table}")
    }
}

impl<'a> Display for ScalarRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarRef::Null => write!(f, "NULL"),
            ScalarRef::EmptyArray => write!(f, "[]"),
            ScalarRef::Int8(i) => write!(f, "{}", i),
            ScalarRef::Int16(i) => write!(f, "{}", i),
            ScalarRef::UInt8(i) => write!(f, "{}", i),
            ScalarRef::UInt16(i) => write!(f, "{}", i),
            ScalarRef::Boolean(b) => write!(f, "{}", b),
            ScalarRef::String(s) => write!(f, "{}", String::from_utf8_lossy(s)),
            ScalarRef::Array(col) => write!(f, "[{}]", col.iter().join(", ")),
            ScalarRef::Tuple(fields) => {
                write!(
                    f,
                    "({})",
                    fields.iter().map(ScalarRef::to_string).join(", ")
                )
            }
        }
    }
}

impl Display for RawExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawExpr::Literal(literal) => write!(f, "{literal}"),
            RawExpr::ColumnRef {
                id,
                data_type,
                property,
            } => write!(f, "ColumnRef({id})::{data_type}{property}"),
            RawExpr::FunctionCall { name, args, params } => {
                write!(f, "{name}")?;
                if !params.is_empty() {
                    write!(f, "(")?;
                    for (i, param) in params.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{param}")?;
                    }
                    write!(f, ")")?;
                }
                write!(f, "(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Boolean(val) => write!(f, "{val}::Boolean"),
            Literal::UInt8(val) => write!(f, "{val}::UInt8"),
            Literal::UInt16(val) => write!(f, "{val}::UInt16"),
            Literal::Int8(val) => write!(f, "{val}::Int8"),
            Literal::Int16(val) => write!(f, "{val}::Int16"),
            Literal::String(val) => write!(f, "{}::String", String::from_utf8_lossy(val)),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::String => write!(f, "String"),
            DataType::UInt8 => write!(f, "UInt8"),
            DataType::UInt16 => write!(f, "UInt16"),
            DataType::Int8 => write!(f, "Int8"),
            DataType::Int16 => write!(f, "Int16"),
            DataType::Null => write!(f, "Nullable(Nothing)"),
            DataType::Nullable(inner) => write!(f, "Nullable({inner})"),
            DataType::EmptyArray => write!(f, "Array(Nothing)"),
            DataType::Array(inner) => write!(f, "Array({inner})"),
            DataType::Tuple(tys) => {
                if tys.len() == 1 {
                    write!(f, "({},)", tys[0])
                } else {
                    write!(f, "(")?;
                    for (i, ty) in tys.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{ty}")?;
                    }
                    write!(f, ")")
                }
            }
            DataType::Generic(index) => write!(f, "T{index}"),
        }
    }
}

impl Display for ValueProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        if self.not_null {
            write!(f, "{{not_null}}")?;
        } else {
            write!(f, "{{}}")?;
        }
        Ok(())
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Literal(literal) => write!(f, "{literal}"),
            Expr::ColumnRef { id } => write!(f, "ColumnRef({id})"),
            Expr::FunctionCall {
                function,
                args,
                generics,
                ..
            } => {
                write!(f, "{}", function.signature.name)?;
                if !generics.is_empty() {
                    write!(f, "<")?;
                    for (i, ty) in generics.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "T{i}={ty}")?;
                    }
                    write!(f, ">")?;
                }
                write!(f, "<")?;
                for (i, ty) in function.signature.args_type.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{ty}")?;
                }
                write!(f, ">")?;
                write!(f, "(")?;
                for (i, (arg, prop)) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}{prop}")?;
                }
                write!(f, ")")
            }
            Expr::Cast { expr, dest_type } => {
                write!(f, "cast<dest_type={dest_type}>({expr})")
            }
        }
    }
}

impl<T: ValueType> Display for Value<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Value::Scalar(scalar) => write!(f, "{:?}", scalar),
            Value::Column(col) => write!(f, "{:?}", col),
        }
    }
}

impl<'a, T: ValueType> Display for ValueRef<'a, T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ValueRef::Scalar(scalar) => write!(f, "{:?}", scalar),
            ValueRef::Column(col) => write!(f, "{:?}", col),
        }
    }
}

impl Debug for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.signature)
    }
}

impl Display for FunctionProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut properties = Vec::new();
        if self.preserve_not_null {
            properties.push("preserve_not_null");
        }
        if self.commutative {
            properties.push("commutative");
        }
        // if self.domain_to_range.is_some() {
        //     properties.push("monotonic");
        // }
        if !properties.is_empty() {
            write!(f, "{{{}}}", properties.join(", "))?;
        }
        Ok(())
    }
}

impl Debug for FunctionProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}
