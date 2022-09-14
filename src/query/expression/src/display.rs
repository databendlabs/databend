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
use std::time::Duration;
use std::time::UNIX_EPOCH;

use bson::Document;
use chrono::DateTime;
use chrono::Utc;
use comfy_table::Cell;
use comfy_table::Table;
use itertools::Itertools;
use num_traits::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;

use crate::chunk::Chunk;
use crate::expression::Expr;
use crate::expression::Literal;
use crate::expression::RawExpr;
use crate::function::Function;
use crate::function::FunctionSignature;
use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;
use crate::types::number::NumberColumn;
use crate::types::number::NumberDataType;
use crate::types::number::NumberDomain;
use crate::types::number::NumberScalar;
use crate::types::number::SimpleDomain;
use crate::types::string::StringColumn;
use crate::types::string::StringDomain;
use crate::types::timestamp::Timestamp;
use crate::types::timestamp::TimestampDomain;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::ValueType;
use crate::values::ScalarRef;
use crate::values::Value;
use crate::values::ValueRef;
use crate::with_number_type;
use crate::Column;

const FLOAT_NUM_FRAC_DIGITS: u32 = 10;

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
                .map(|val| val.as_ref().index(index).unwrap().to_string())
                .map(Cell::new)
                .collect();
            table.add_row(row);
        }
        write!(f, "{table}")
    }
}

impl<'a> Debug for ScalarRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarRef::Null => write!(f, "NULL"),
            ScalarRef::EmptyArray => write!(f, "[]"),
            ScalarRef::Number(val) => write!(f, "{val:?}"),
            ScalarRef::Boolean(val) => write!(f, "{val}"),
            ScalarRef::String(s) => write!(f, "{:?}", String::from_utf8_lossy(s)),
            ScalarRef::Timestamp(t) => write!(f, "{t:?}"),
            ScalarRef::Array(col) => write!(f, "[{}]", col.iter().join(", ")),
            ScalarRef::Tuple(fields) => {
                write!(
                    f,
                    "({})",
                    fields.iter().map(ScalarRef::to_string).join(", ")
                )
            }
            ScalarRef::Variant(s) => write!(f, "0x{}", &hex::encode(s)),
        }
    }
}

impl Debug for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::Null { len } => f.debug_struct("Null").field("len", len).finish(),
            Column::EmptyArray { len } => f.debug_struct("EmptyArray").field("len", len).finish(),
            Column::Number(col) => write!(f, "{col:?}"),
            Column::Boolean(col) => f.debug_tuple("Boolean").field(col).finish(),
            Column::String(col) => write!(f, "{col:?}"),
            Column::Timestamp(col) => write!(f, "{col:?}"),
            Column::Array(col) => write!(f, "{col:?}"),
            Column::Nullable(col) => write!(f, "{col:?}"),
            Column::Tuple { fields, len } => f
                .debug_struct("Tuple")
                .field("fields", fields)
                .field("len", len)
                .finish(),
            Column::Variant(col) => write!(f, "{col:?}"),
        }
    }
}

impl<'a> Display for ScalarRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarRef::Null => write!(f, "NULL"),
            ScalarRef::EmptyArray => write!(f, "[]"),
            ScalarRef::Number(val) => write!(f, "{val}"),
            ScalarRef::Boolean(val) => write!(f, "{val}"),
            ScalarRef::String(s) => write!(f, "{:?}", String::from_utf8_lossy(s)),
            ScalarRef::Timestamp(t) => write!(f, "{t}"),
            ScalarRef::Array(col) => write!(f, "[{}]", col.iter().join(", ")),
            ScalarRef::Tuple(fields) => {
                write!(
                    f,
                    "({})",
                    fields.iter().map(ScalarRef::to_string).join(", ")
                )
            }
            ScalarRef::Variant(s) => {
                let doc = Document::from_reader(*s).map_err(|_| std::fmt::Error)?;
                let bson = doc.get("v").ok_or(std::fmt::Error)?;
                write!(f, "{bson}")
            }
        }
    }
}

impl Debug for NumberScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberScalar::UInt8(val) => write!(f, "{val}_u8"),
            NumberScalar::UInt16(val) => write!(f, "{val}_u16"),
            NumberScalar::UInt32(val) => write!(f, "{val}_u32"),
            NumberScalar::UInt64(val) => write!(f, "{val}_u64"),
            NumberScalar::Int8(val) => write!(f, "{val}_i8"),
            NumberScalar::Int16(val) => write!(f, "{val}_i16"),
            NumberScalar::Int32(val) => write!(f, "{val}_i32"),
            NumberScalar::Int64(val) => write!(f, "{val}_i64"),
            NumberScalar::Float32(val) => match Decimal::from_f32(val.0) {
                Some(d) => write!(
                    f,
                    "{}_f32",
                    d.round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
                        .normalize()
                ),
                None => write!(f, "{val}_f32"),
            },
            NumberScalar::Float64(val) => match Decimal::from_f64(val.0) {
                Some(d) => write!(
                    f,
                    "{}_f64",
                    d.round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
                        .normalize()
                ),
                None => write!(f, "{val}_f64"),
            },
        }
    }
}

impl Display for NumberScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberScalar::UInt8(val) => write!(f, "{val}"),
            NumberScalar::UInt16(val) => write!(f, "{val}"),
            NumberScalar::UInt32(val) => write!(f, "{val}"),
            NumberScalar::UInt64(val) => write!(f, "{val}"),
            NumberScalar::Int8(val) => write!(f, "{val}"),
            NumberScalar::Int16(val) => write!(f, "{val}"),
            NumberScalar::Int32(val) => write!(f, "{val}"),
            NumberScalar::Int64(val) => write!(f, "{val}"),
            NumberScalar::Float32(val) => match Decimal::from_f32(val.0) {
                Some(d) => write!(
                    f,
                    "{}",
                    d.round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
                        .normalize()
                ),
                None => write!(f, "{val}"),
            },
            NumberScalar::Float64(val) => match Decimal::from_f64(val.0) {
                Some(d) => write!(
                    f,
                    "{}",
                    d.round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
                        .normalize()
                ),
                None => write!(f, "{val}"),
            },
        }
    }
}

impl Debug for NumberColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberColumn::UInt8(val) => f.debug_tuple("UInt8").field(val).finish(),
            NumberColumn::UInt16(val) => f.debug_tuple("UInt16").field(val).finish(),
            NumberColumn::UInt32(val) => f.debug_tuple("UInt32").field(val).finish(),
            NumberColumn::UInt64(val) => f.debug_tuple("UInt64").field(val).finish(),
            NumberColumn::Int8(val) => f.debug_tuple("Int8").field(val).finish(),
            NumberColumn::Int16(val) => f.debug_tuple("Int16").field(val).finish(),
            NumberColumn::Int32(val) => f.debug_tuple("Int32").field(val).finish(),
            NumberColumn::Int64(val) => f.debug_tuple("Int64").field(val).finish(),
            NumberColumn::Float32(val) => f
                .debug_tuple("Float32")
                .field(&format_args!(
                    "[{}]",
                    &val.iter()
                        .map(|x| match Decimal::from_f32(x.0) {
                            Some(d) => d
                                .round_dp_with_strategy(
                                    FLOAT_NUM_FRAC_DIGITS,
                                    RoundingStrategy::ToZero
                                )
                                .normalize()
                                .to_string(),
                            None => x.to_string(),
                        })
                        .join(", ")
                ))
                .finish(),
            NumberColumn::Float64(val) => f
                .debug_tuple("Float64")
                .field(&format_args!(
                    "[{}]",
                    &val.iter()
                        .map(|x| match Decimal::from_f64(x.0) {
                            Some(d) => d
                                .round_dp_with_strategy(
                                    FLOAT_NUM_FRAC_DIGITS,
                                    RoundingStrategy::ToZero
                                )
                                .normalize()
                                .to_string(),
                            None => x.to_string(),
                        })
                        .join(", ")
                ))
                .finish(),
        }
    }
}

impl Debug for StringColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringColumn")
            .field("data", &format_args!("0x{}", &hex::encode(&*self.data)))
            .field("offsets", &self.offsets)
            .finish()
    }
}

impl Display for RawExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawExpr::Literal { lit, .. } => write!(f, "{lit}"),
            RawExpr::ColumnRef { id, data_type, .. } => write!(f, "ColumnRef({id})::{data_type}"),
            RawExpr::Cast {
                expr, dest_type, ..
            } => {
                write!(f, "CAST({expr} AS {dest_type})")
            }
            RawExpr::TryCast {
                expr, dest_type, ..
            } => {
                write!(f, "TRY_CAST({expr} AS {dest_type})")
            }
            RawExpr::FunctionCall {
                name, args, params, ..
            } => {
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
            Literal::Boolean(val) => write!(f, "{val}"),
            Literal::UInt64(val) => write!(f, "{val}_u64"),
            Literal::Int8(val) => write!(f, "{val}_i8"),
            Literal::Int16(val) => write!(f, "{val}_i16"),
            Literal::Int32(val) => write!(f, "{val}_i32"),
            Literal::Int64(val) => write!(f, "{val}_i64"),
            Literal::UInt8(val) => write!(f, "{val}_u8"),
            Literal::UInt16(val) => write!(f, "{val}_u16"),
            Literal::UInt32(val) => write!(f, "{val}_u32"),
            Literal::Float32(val) => write!(f, "{val}_f32"),
            Literal::Float64(val) => write!(f, "{val}_f64"),
            Literal::String(val) => write!(f, "{:?}", String::from_utf8_lossy(val)),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::String => write!(f, "String"),
            DataType::Number(num) => write!(f, "{num}"),
            DataType::Timestamp => write!(f, "Timestamp"),
            DataType::Null => write!(f, "NULL"),
            DataType::Nullable(inner) => write!(f, "{inner} NULL"),
            DataType::EmptyArray => write!(f, "Array(Nothing)"),
            DataType::Array(inner) => write!(f, "Array({inner})"),
            DataType::Map(inner) => write!(f, "Map({inner})"),
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
            DataType::Variant => write!(f, "Variant"),
            DataType::Generic(index) => write!(f, "T{index}"),
        }
    }
}

impl Display for NumberDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self {
            NumberDataType::UInt8 => write!(f, "UInt8"),
            NumberDataType::UInt16 => write!(f, "UInt16"),
            NumberDataType::UInt32 => write!(f, "UInt32"),
            NumberDataType::UInt64 => write!(f, "UInt64"),
            NumberDataType::Int8 => write!(f, "Int8"),
            NumberDataType::Int16 => write!(f, "Int16"),
            NumberDataType::Int32 => write!(f, "Int32"),
            NumberDataType::Int64 => write!(f, "Int64"),
            NumberDataType::Float32 => write!(f, "Float32"),
            NumberDataType::Float64 => write!(f, "Float64"),
        }
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Constant { scalar, .. } => write!(f, "{:?}", scalar.as_ref()),
            Expr::ColumnRef { id, .. } => write!(f, "ColumnRef({id})"),
            Expr::Cast {
                expr, dest_type, ..
            } => {
                write!(f, "CAST({expr} AS {dest_type})")
            }
            Expr::TryCast {
                expr, dest_type, ..
            } => {
                write!(f, "TRY_CAST({expr} AS {dest_type})")
            }
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

impl Display for FunctionSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}) :: {}",
            self.name,
            self.args_type.iter().map(|t| t.to_string()).join(", "),
            self.return_type
        )
    }
}

impl Display for FunctionProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut properties = Vec::new();
        if self.commutative {
            properties.push("commutative");
        }
        if !properties.is_empty() {
            write!(f, "{{{}}}", properties.join(", "))?;
        }
        Ok(())
    }
}

impl Display for NullableDomain<AnyType> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(value) = &self.value {
            if self.has_null {
                write!(f, "{} ∪ {{NULL}}", value)
            } else {
                write!(f, "{}", value)
            }
        } else {
            assert!(self.has_null);
            write!(f, "{{NULL}}")
        }
    }
}

impl Display for BooleanDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.has_false && self.has_true {
            write!(f, "{{FALSE, TRUE}}")
        } else if self.has_false {
            write!(f, "{{FALSE}}")
        } else {
            write!(f, "{{TRUE}}")
        }
    }
}

impl Display for StringDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(max) = &self.max {
            write!(
                f,
                "{{{:?}..={:?}}}",
                String::from_utf8_lossy(&self.min),
                String::from_utf8_lossy(max)
            )
        } else {
            write!(f, "{{{:?}..}}", String::from_utf8_lossy(&self.min))
        }
    }
}

impl Display for TimestampDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{{:.prec$}..={:.prec$}}}",
            self.min,
            self.max,
            prec = self.precision as usize
        )
    }
}

impl Display for NumberDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        with_number_type!(|TYPE| match self {
            NumberDomain::TYPE(domain) => write!(f, "{domain}"),
        })
    }
}

impl<T: Display> Display for SimpleDomain<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{{}..={}}}", self.min, self.max)
    }
}
impl Display for Domain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Domain::Number(domain) => write!(f, "{domain}"),
            Domain::Boolean(domain) => write!(f, "{domain}"),
            Domain::String(domain) => write!(f, "{domain}"),
            Domain::Timestamp(domain) => write!(f, "{domain}"),
            Domain::Nullable(domain) => write!(f, "{domain}"),
            Domain::Array(None) => write!(f, "[]"),
            Domain::Array(Some(domain)) => write!(f, "[{domain}]"),
            Domain::Tuple(fields) => {
                write!(f, "(")?;
                for (i, domain) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{domain}")?;
                }
                write!(f, ")")
            }
            Domain::Undefined => write!(f, "_"),
        }
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let dt = if self.ts >= 0 {
            DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_micros(self.ts.unsigned_abs()))
        } else {
            DateTime::<Utc>::from(UNIX_EPOCH - Duration::from_micros(self.ts.unsigned_abs()))
        };
        let s = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let truncate_end = if self.precision > 0 {
            s.len() - 6 + self.precision as usize
        } else {
            s.len() - 7
        };
        write!(f, "{}", &s[..truncate_end])?;
        Ok(())
    }
}
