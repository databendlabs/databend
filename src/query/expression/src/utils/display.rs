// Copyright 2021 Datafuse Labs
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
use std::fmt::Write;

use comfy_table::Cell;
use comfy_table::Table;
use databend_common_ast::ast::quote::display_ident;
use databend_common_ast::ast::quote::QuotedString;
use databend_common_ast::parser::Dialect;
use databend_common_column::binary::BinaryColumn;
use databend_common_io::deserialize_bitmap;
use databend_common_io::display_decimal_128;
use databend_common_io::display_decimal_256;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkt;
use geozero::wkb::Ewkb;
use itertools::Itertools;
use jiff::tz::TimeZone;
use jsonb::RawJsonb;
use num_traits::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;

use crate::block::DataBlock;
use crate::expr::*;
use crate::expression::Expr;
use crate::expression::RawExpr;
use crate::function::Function;
use crate::function::FunctionSignature;
use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::types::boolean::BooleanDomain;
use crate::types::date::date_to_string;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalDataType;
use crate::types::decimal::DecimalDomain;
use crate::types::decimal::DecimalScalar;
use crate::types::interval::interval_to_string;
use crate::types::map::KvPair;
use crate::types::nullable::NullableDomain;
use crate::types::number::NumberColumn;
use crate::types::number::NumberDataType;
use crate::types::number::NumberDomain;
use crate::types::number::NumberScalar;
use crate::types::number::SimpleDomain;
use crate::types::opaque::OpaqueColumn;
use crate::types::opaque::OpaqueScalarRef;
use crate::types::string::StringDomain;
use crate::types::timestamp::timestamp_to_string;
use crate::types::vector::VectorDataType;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::NumberClass;
use crate::types::ValueType;
use crate::types::VectorScalarRef;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::values::Value;
use crate::visit_expr;
use crate::with_integer_mapped_type;
use crate::with_opaque_type;
use crate::with_vector_number_type;
use crate::Column;
use crate::ColumnIndex;
use crate::ExprVisitor;
use crate::FunctionEval;
use crate::TableDataType;
use crate::VariantDataType;

const FLOAT_NUM_FRAC_DIGITS: u32 = 10;

impl Debug for DataBlock {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        table.set_header(vec!["Column ID", "Type", "Column Data"]);

        for (i, entry) in self.columns().iter().enumerate() {
            table.add_row(vec![
                i.to_string(),
                entry.data_type().to_string(),
                format!("{:?}", entry.value()),
            ]);
        }

        write!(f, "{}", table)
    }
}

impl Display for DataBlock {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        table.set_header((0..self.num_columns()).map(|idx| format!("Column {idx}")));

        for index in 0..self.num_rows() {
            let row: Vec<_> = self
                .columns()
                .iter()
                .map(|entry| entry.index(index).unwrap().to_string())
                .map(Cell::new)
                .collect();
            table.add_row(row);
        }
        write!(f, "{table}")
    }
}

impl Debug for ScalarRef<'_> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ScalarRef::Null => write!(f, "NULL"),
            ScalarRef::EmptyArray => write!(f, "[] :: Array(Nothing)"),
            ScalarRef::EmptyMap => write!(f, "{{}} :: Map(Nothing)"),
            ScalarRef::Number(val) => write!(f, "{val:?}"),
            ScalarRef::Decimal(val) => write!(f, "{val:?}"),
            ScalarRef::Boolean(val) => write!(f, "{val}"),
            ScalarRef::Binary(s) => {
                for c in *s {
                    write!(f, "{:02X}", c)?;
                }
                Ok(())
            }
            ScalarRef::String(s) => write!(f, "{s:?}"),
            ScalarRef::Timestamp(t) => write!(f, "{t:?}"),
            ScalarRef::TimestampTimezone(t) => write!(f, "{t:?}"),
            ScalarRef::Date(d) => write!(f, "{d:?}"),
            ScalarRef::Interval(i) => {
                let interval = interval_to_string(i);
                write!(f, "{interval}")
            }
            ScalarRef::Array(col) => write!(f, "[{}]", col.iter().join(", ")),
            ScalarRef::Map(col) => {
                write!(f, "{{")?;
                let kv_col = KvPair::<AnyType, AnyType>::try_downcast_column(col).unwrap();
                for (i, (key, value)) in kv_col.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{key:?}")?;
                    write!(f, ":")?;
                    write!(f, "{value:?}")?;
                }
                write!(f, "}}")
            }
            ScalarRef::Bitmap(bits) => {
                let rb = deserialize_bitmap(bits).unwrap();
                write!(f, "{rb:?}")
            }
            ScalarRef::Tuple(fields) => {
                write!(f, "(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{field:?}")?;
                }
                if fields.len() < 2 {
                    write!(f, ",")?;
                }
                write!(f, ")")
            }
            ScalarRef::Variant(s) => {
                write!(f, "0x")?;
                for c in *s {
                    write!(f, "{c:02x}")?;
                }
                Ok(())
            }
            ScalarRef::Geometry(s) => {
                let geom = ewkb_to_geo(&mut Ewkb(s))
                    .and_then(|(geo, srid)| geo_to_ewkt(geo, srid))
                    .unwrap_or_else(|e| format!("GeozeroError: {:?}", e));
                write!(f, "{geom:?}")
            }
            ScalarRef::Geography(v) => {
                let geog = ewkb_to_geo(&mut Ewkb(v.0))
                    .and_then(|(geo, srid)| geo_to_ewkt(geo, srid))
                    .unwrap_or_else(|e| format!("GeozeroError: {:?}", e));
                write!(f, "{geog:?}")
            }
            ScalarRef::Vector(col) => with_vector_number_type!(|NUM_TYPE| match col {
                VectorScalarRef::NUM_TYPE(vals) => {
                    write!(f, "[{}]", vals.iter().join(", "))
                }
            }),
            ScalarRef::Opaque(opaque) => {
                with_opaque_type!(|T| match opaque {
                    OpaqueScalarRef::T(vals) => {
                        write!(
                            f,
                            "Opaque({})",
                            vals.iter().map(|x| format!("{:x}", x)).join(",")
                        )
                    }
                })
            }
        }
    }
}

impl Debug for Column {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        struct FmtBinary<'a>(&'a [u8]);
        impl Debug for FmtBinary<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "0x{}", hex::encode(self.0))
            }
        }
        fn fmt_binary(f: &mut Formatter<'_>, name: &str, col: &BinaryColumn) -> std::fmt::Result {
            f.debug_tuple(name)
                .field_with(|f| f.debug_list().entries(col.iter().map(FmtBinary)).finish())
                .finish()
        }

        fn fmt_opaque(f: &mut Formatter<'_>, col: &OpaqueColumn) -> std::fmt::Result {
            f.debug_tuple("Opaque")
                .field_with(|f| {
                    f.debug_list()
                        .entries((0..col.len()).map(|i| {
                            let scalar = unsafe { col.index_unchecked(i) };
                            format!("0x{}", hex::encode_upper(scalar.to_le_bytes()))
                        }))
                        .finish()
                })
                .finish()
        }
        match self {
            Column::Null { len } => f.debug_struct("Null").field("len", len).finish(),
            Column::EmptyArray { len } => f.debug_struct("EmptyArray").field("len", len).finish(),
            Column::EmptyMap { len } => f.debug_struct("EmptyMap").field("len", len).finish(),
            Column::Number(col) => write!(f, "{col:?}"),
            Column::Decimal(col) => write!(f, "{col:?}"),
            Column::Boolean(col) => f.debug_tuple("Boolean").field(col).finish(),
            Column::Binary(col) => write!(f, "{col:?}"),
            Column::String(col) => write!(f, "{col:?}"),
            Column::Timestamp(col) => f.debug_tuple("Timestamp").field(col).finish(),
            Column::TimestampTimezone(col) => {
                f.debug_tuple("TimestampTimezone").field(col).finish()
            }
            Column::Date(col) => f.debug_tuple("Date").field(col).finish(),
            Column::Interval(col) => write!(f, "{col:?}"),
            Column::Array(col) => write!(f, "{col:?}"),
            Column::Map(col) => write!(f, "{col:?}"),
            Column::Bitmap(col) => fmt_binary(f, "Bitmap", col),
            Column::Nullable(col) => write!(f, "{col:?}"),
            Column::Tuple(fields) => f.debug_tuple("Tuple").field(fields).finish(),
            Column::Variant(col) => fmt_binary(f, "Variant", col),
            Column::Geometry(col) => fmt_binary(f, "Geometry", col),
            Column::Geography(col) => write!(f, "{col:?}"),
            Column::Vector(col) => write!(f, "{col:?}"),
            Column::Opaque(col) => fmt_opaque(f, col),
        }
    }
}

impl Display for ScalarRef<'_> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ScalarRef::Null => write!(f, "NULL"),
            ScalarRef::EmptyArray => write!(f, "[]"),
            ScalarRef::EmptyMap => write!(f, "{{}}"),
            ScalarRef::Number(val) => write!(f, "{val}"),
            ScalarRef::Decimal(val) => write!(f, "{val}"),
            ScalarRef::Boolean(val) => write!(f, "{val}"),
            ScalarRef::Binary(s) => {
                for c in *s {
                    write!(f, "{c:02X}")?;
                }
                Ok(())
            }
            ScalarRef::Opaque(o) => write!(f, "{o:?}"),
            ScalarRef::String(s) => write!(f, "{}", QuotedString(s, '\'')),
            ScalarRef::Timestamp(t) => write!(f, "'{}'", timestamp_to_string(*t, &TimeZone::UTC)),
            ScalarRef::TimestampTimezone(t) => write!(f, "'{}'", t),
            ScalarRef::Date(d) => write!(f, "'{}'", date_to_string(*d as i64, &TimeZone::UTC)),
            ScalarRef::Interval(interval) => write!(f, "'{}'", interval_to_string(interval)),
            ScalarRef::Array(col) => write!(f, "[{}]", col.iter().join(", ")),
            ScalarRef::Map(col) => {
                write!(f, "{{")?;
                let kv_col = KvPair::<AnyType, AnyType>::try_downcast_column(col).unwrap();
                for (i, (key, value)) in kv_col.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{key}")?;
                    write!(f, ":")?;
                    write!(f, "{value}")?;
                }
                write!(f, "}}")
            }
            ScalarRef::Bitmap(bits) => {
                let rb = deserialize_bitmap(bits).unwrap();
                write!(f, "'{}'", rb.into_iter().join(","))
            }
            ScalarRef::Tuple(fields) => {
                write!(f, "(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{field}")?;
                }
                if fields.len() < 2 {
                    write!(f, ",")?;
                }
                write!(f, ")")
            }
            ScalarRef::Variant(s) => {
                let raw_jsonb = RawJsonb::new(s);
                let value = raw_jsonb.to_string();
                write!(f, "{}", QuotedString(value, '\''))
            }
            ScalarRef::Geometry(s) => {
                let geom = ewkb_to_geo(&mut Ewkb(s))
                    .and_then(|(geo, srid)| geo_to_ewkt(geo, srid))
                    .unwrap_or_else(|e| format!("GeozeroError: {:?}", e));
                write!(f, "{}", QuotedString(geom, '\''))
            }
            ScalarRef::Geography(v) => {
                let geog = ewkb_to_geo(&mut Ewkb(v.0))
                    .and_then(|(geo, srid)| geo_to_ewkt(geo, srid))
                    .unwrap_or_else(|e| format!("GeozeroError: {:?}", e));
                write!(f, "{}", QuotedString(geog, '\''))
            }
            ScalarRef::Vector(col) => with_vector_number_type!(|NUM_TYPE| match col {
                VectorScalarRef::NUM_TYPE(vals) => {
                    write!(f, "[{}]", vals.iter().join(", "))
                }
            }),
        }
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

// convert scalar value to string without quotes
pub fn scalar_ref_to_string(value: &ScalarRef) -> String {
    match value {
        ScalarRef::String(s) => s.to_string(),
        ScalarRef::Timestamp(t) => format!("{}", timestamp_to_string(*t, &TimeZone::UTC)),
        ScalarRef::Date(d) => format!("{}", date_to_string(*d as i64, &TimeZone::UTC)),
        ScalarRef::Interval(interval) => format!("{}", interval_to_string(interval)),
        ScalarRef::Bitmap(bits) => {
            let rb = deserialize_bitmap(bits).unwrap();
            format!("{}", rb.into_iter().join(","))
        }
        ScalarRef::Variant(s) => {
            let raw_jsonb = RawJsonb::new(s);
            raw_jsonb.to_string()
        }
        ScalarRef::Geometry(s) => {
            let geom = ewkb_to_geo(&mut Ewkb(s))
                .and_then(|(geo, srid)| geo_to_ewkt(geo, srid))
                .unwrap_or_else(|e| format!("GeozeroError: {:?}", e));
            format!("{}", geom)
        }
        ScalarRef::Geography(v) => {
            let geog = ewkb_to_geo(&mut Ewkb(v.0))
                .and_then(|(geo, srid)| geo_to_ewkt(geo, srid))
                .unwrap_or_else(|e| format!("GeozeroError: {:?}", e));
            format!("{}", geog)
        }
        _ => format!("{}", value),
    }
}

impl Debug for NumberScalar {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            NumberScalar::UInt8(val) => write!(f, "{val}_u8"),
            NumberScalar::UInt16(val) => write!(f, "{val}_u16"),
            NumberScalar::UInt32(val) => write!(f, "{val}_u32"),
            NumberScalar::UInt64(val) => write!(f, "{val}_u64"),
            NumberScalar::Int8(val) => write!(f, "{val}_i8"),
            NumberScalar::Int16(val) => write!(f, "{val}_i16"),
            NumberScalar::Int32(val) => write!(f, "{val}_i32"),
            NumberScalar::Int64(val) => write!(f, "{val}_i64"),
            NumberScalar::Float32(val) => write!(f, "{}_f32", display_f32(**val)),
            NumberScalar::Float64(val) => write!(f, "{}_f64", display_f64(**val)),
        }
    }
}

impl Display for NumberScalar {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            NumberScalar::UInt8(val) => write!(f, "{val}"),
            NumberScalar::UInt16(val) => write!(f, "{val}"),
            NumberScalar::UInt32(val) => write!(f, "{val}"),
            NumberScalar::UInt64(val) => write!(f, "{val}"),
            NumberScalar::Int8(val) => write!(f, "{val}"),
            NumberScalar::Int16(val) => write!(f, "{val}"),
            NumberScalar::Int32(val) => write!(f, "{val}"),
            NumberScalar::Int64(val) => write!(f, "{val}"),
            NumberScalar::Float32(val) => write!(f, "{}", display_f32(**val)),
            NumberScalar::Float64(val) => write!(f, "{}", display_f64(**val)),
        }
    }
}

impl Debug for DecimalScalar {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            DecimalScalar::Decimal64(val, size) => {
                write!(
                    f,
                    "{}_d64({},{})",
                    display_decimal_128(*val as i128, size.scale()),
                    size.precision(),
                    size.scale()
                )
            }
            DecimalScalar::Decimal128(val, size) => {
                write!(
                    f,
                    "{}_d128({},{})",
                    display_decimal_128(*val, size.scale()),
                    size.precision(),
                    size.scale()
                )
            }
            DecimalScalar::Decimal256(val, size) => {
                write!(
                    f,
                    "{}_d256({},{})",
                    display_decimal_256(val.0, size.scale()),
                    size.precision(),
                    size.scale()
                )
            }
        }
    }
}

impl Display for DecimalScalar {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            DecimalScalar::Decimal64(val, size) => {
                write!(f, "{}", display_decimal_128(*val as i128, size.scale()))
            }
            DecimalScalar::Decimal128(val, size) => {
                write!(f, "{}", display_decimal_128(*val, size.scale()))
            }
            DecimalScalar::Decimal256(val, size) => {
                write!(f, "{}", display_decimal_256(val.0, size.scale()))
            }
        }
    }
}

impl Debug for NumberColumn {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
                    &val.iter().map(|x| display_f32(x.0)).join(", ")
                ))
                .finish(),
            NumberColumn::Float64(val) => f
                .debug_tuple("Float64")
                .field(&format_args!(
                    "[{}]",
                    &val.iter().map(|x| display_f64(x.0)).join(", ")
                ))
                .finish(),
        }
    }
}

impl Debug for DecimalColumn {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            DecimalColumn::Decimal64(val, size) => f
                .debug_tuple("Decimal64")
                .field_with(|f| {
                    f.debug_list()
                        .entries(
                            val.iter()
                                .map(|x| display_decimal_128(*x as i128, size.scale())),
                        )
                        .finish()
                })
                .finish(),
            DecimalColumn::Decimal128(val, size) => f
                .debug_tuple("Decimal128")
                .field_with(|f| {
                    f.debug_list()
                        .entries(val.iter().map(|x| display_decimal_128(*x, size.scale())))
                        .finish()
                })
                .finish(),
            DecimalColumn::Decimal256(val, size) => f
                .debug_tuple("Decimal256")
                .field_with(|f| {
                    f.debug_list()
                        .entries(val.iter().map(|x| display_decimal_256(x.0, size.scale())))
                        .finish()
                })
                .finish(),
        }
    }
}

impl<Index: ColumnIndex> Display for RawExpr<Index> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            RawExpr::Constant { scalar, .. } => write!(f, "{scalar}"),
            RawExpr::ColumnRef {
                display_name,
                data_type,
                ..
            } => {
                write!(f, "{}::{}", display_name, data_type)
            }
            RawExpr::Cast {
                is_try,
                expr,
                dest_type,
                ..
            } => {
                if *is_try {
                    write!(f, "TRY_CAST({expr} AS {dest_type})")
                } else {
                    write!(f, "CAST({expr} AS {dest_type})")
                }
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
            RawExpr::LambdaFunctionCall {
                name,
                args,
                lambda_display,
                ..
            } => {
                write!(f, "{name}")?;
                write!(f, "(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ", ")?;
                write!(f, "{lambda_display}")?;
                write!(f, ")")
            }
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Binary => write!(f, "Binary"),
            DataType::String => write!(f, "String"),
            DataType::Number(num) => write!(f, "{num}"),
            DataType::Decimal(decimal) => write!(f, "{decimal}"),
            DataType::Timestamp => write!(f, "Timestamp"),
            DataType::Date => write!(f, "Date"),
            DataType::Interval => write!(f, "Interval"),
            DataType::Null => write!(f, "NULL"),
            DataType::Nullable(inner) => write!(f, "{inner} NULL"),
            DataType::EmptyArray => write!(f, "Array(Nothing)"),
            DataType::Array(inner) => write!(f, "Array({inner})"),
            DataType::EmptyMap => write!(f, "Map(Nothing)"),
            DataType::Map(inner) => match *inner.clone() {
                DataType::Tuple(fields) => {
                    write!(f, "Map({}, {})", fields[0], fields[1])
                }
                _ => unreachable!(),
            },
            DataType::Bitmap => write!(f, "Bitmap"),
            DataType::Tuple(tys) => {
                write!(f, "Tuple(")?;
                for (i, ty) in tys.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{ty}")?;
                }
                if tys.len() == 1 {
                    write!(f, ",")?;
                }
                write!(f, ")")
            }
            DataType::Variant => write!(f, "Variant"),
            DataType::Geometry => write!(f, "Geometry"),
            DataType::Geography => write!(f, "Geography"),
            DataType::Vector(vector) => write!(f, "{vector}"),
            DataType::Generic(index) => write!(f, "T{index}"),
            DataType::Opaque(size) => write!(f, "Opaque({size})"),
            DataType::StageLocation => write!(f, "StageLocation"),
            DataType::TimestampTimezone => write!(f, "TimestampTimezone"),
        }
    }
}

impl Display for TableDataType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match &self {
            TableDataType::Boolean => write!(f, "Boolean"),
            TableDataType::Binary => write!(f, "Binary"),
            TableDataType::Opaque(size) => write!(f, "Opaque({size})"),
            TableDataType::String => write!(f, "String"),
            TableDataType::Number(num) => write!(f, "{num}"),
            TableDataType::Decimal(decimal) => write!(f, "{decimal}"),
            TableDataType::Timestamp => write!(f, "Timestamp"),
            TableDataType::TimestampTimezone => write!(f, "TimestampTimezone"),
            TableDataType::Date => write!(f, "Date"),
            TableDataType::Null => write!(f, "NULL"),
            TableDataType::Nullable(inner) => write!(f, "{inner} NULL"),
            TableDataType::EmptyArray => write!(f, "Array(Nothing)"),
            TableDataType::Array(inner) => write!(f, "Array({inner})"),
            TableDataType::EmptyMap => write!(f, "Map(Nothing)"),
            TableDataType::Map(inner) => match *inner.clone() {
                TableDataType::Tuple {
                    fields_name: _fields_name,
                    fields_type,
                } => {
                    write!(f, "Map({}, {})", fields_type[0], fields_type[1])
                }
                _ => unreachable!(),
            },
            TableDataType::Bitmap => write!(f, "Bitmap"),
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                write!(f, "Tuple(")?;
                for (i, (name, ty)) in fields_name.iter().zip(fields_type).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{name} {ty}")?;
                }
                if fields_name.len() == 1 {
                    write!(f, ",")?;
                }
                write!(f, ")")
            }
            TableDataType::Variant => write!(f, "Variant"),
            TableDataType::Interval => write!(f, "Interval"),
            TableDataType::Geometry => write!(f, "Geometry"),
            TableDataType::Geography => write!(f, "Geography"),
            TableDataType::Vector(vector) => write!(f, "{vector}"),
            TableDataType::StageLocation => write!(f, "StageLocation"),
        }
    }
}

impl Display for NumberDataType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

impl Display for DecimalDataType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Decimal({}, {})", self.precision(), self.scale())
    }
}

impl Display for DecimalSize {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Decimal({}, {})", self.precision(), self.scale())
    }
}

impl Display for VariantDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            VariantDataType::Jsonb => write!(f, "Jsonb"),
            VariantDataType::Boolean => write!(f, "Boolean"),
            VariantDataType::UInt64 => write!(f, "UInt64"),
            VariantDataType::Int64 => write!(f, "Int64"),
            VariantDataType::Float64 => write!(f, "Float64"),
            VariantDataType::String => write!(f, "String"),
            VariantDataType::Array(inner) => write!(f, "Array({inner})"),
            VariantDataType::Decimal(inner) => write!(f, "Decimal({inner})"),
            VariantDataType::Binary => write!(f, "Binary"),
            VariantDataType::Date => write!(f, "Date"),
            VariantDataType::Timestamp => write!(f, "Timestamp"),
            VariantDataType::Interval => write!(f, "Interval"),
        }
    }
}

impl Display for VectorDataType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match &self {
            VectorDataType::Int8(dim) => write!(f, "Vector({dim})"),
            VectorDataType::Float32(dim) => write!(f, "Vector({dim})"),
        }
    }
}

impl Display for NumberClass {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match &self {
            NumberClass::UInt8 => write!(f, "UInt8"),
            NumberClass::UInt16 => write!(f, "UInt16"),
            NumberClass::UInt32 => write!(f, "UInt32"),
            NumberClass::UInt64 => write!(f, "UInt64"),
            NumberClass::Int8 => write!(f, "Int8"),
            NumberClass::Int16 => write!(f, "Int16"),
            NumberClass::Int32 => write!(f, "Int32"),
            NumberClass::Int64 => write!(f, "Int64"),
            NumberClass::Decimal128 => write!(f, "Decimal128"),
            NumberClass::Decimal256 => write!(f, "Decimal256"),
            NumberClass::Float32 => write!(f, "Float32"),
            NumberClass::Float64 => write!(f, "Float64"),
        }
    }
}

impl<Index: ColumnIndex> Display for Expr<Index> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut visitor = ExprFormatter { f, with_id: false };
        visit_expr(self, &mut visitor)?;
        Ok(())
    }
}

pub struct FmtExpr<'a, I: ColumnIndex> {
    expr: &'a Expr<I>,
    with_id: bool,
}

impl<I: ColumnIndex> Display for FmtExpr<'_, I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut visitor = ExprFormatter {
            f,
            with_id: self.with_id,
        };
        visit_expr(self.expr, &mut visitor)?;
        Ok(())
    }
}

impl<I: ColumnIndex> Expr<I> {
    pub fn fmt_with_options(&self, with_id: bool) -> FmtExpr<'_, I> {
        FmtExpr {
            expr: self,
            with_id,
        }
    }
}

struct ExprFormatter<'a, 'b> {
    f: &'a mut std::fmt::Formatter<'b>,
    with_id: bool,
}

impl std::fmt::Write for ExprFormatter<'_, '_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.f.write_str(s)
    }

    fn write_char(&mut self, c: char) -> std::fmt::Result {
        self.f.write_char(c)
    }

    fn write_fmt(&mut self, args: std::fmt::Arguments<'_>) -> std::fmt::Result {
        self.f.write_fmt(args)
    }
}

impl<I: ColumnIndex> ExprVisitor<I> for ExprFormatter<'_, '_> {
    type Error = std::fmt::Error;

    fn enter_constant(&mut self, expr: &Constant) -> Result<Option<Expr<I>>, Self::Error> {
        write!(self, "{:?}", expr.scalar.as_ref())?;
        Ok(None)
    }

    fn enter_column_ref(&mut self, expr: &ColumnRef<I>) -> Result<Option<Expr<I>>, Self::Error> {
        let ColumnRef {
            display_name, id, ..
        } = expr;
        self.write_str(display_name)?;
        if self.with_id {
            self.write_str(" (#")?;
            id.unique_name(self)?;
            self.write_char(')')?;
        }
        Ok(None)
    }

    fn enter_cast(&mut self, expr: &Cast<I>) -> Result<Option<Expr<I>>, Self::Error> {
        let Cast {
            is_try,
            expr,
            dest_type,
            ..
        } = expr;
        if *is_try {
            self.write_str("TRY_")?;
        }
        write!(self, "CAST<{}>(", expr.data_type())?;
        visit_expr(expr, self)?;
        write!(self, " AS {dest_type})")?;
        Ok(None)
    }

    fn enter_function_call(
        &mut self,
        expr: &FunctionCall<I>,
    ) -> Result<Option<Expr<I>>, Self::Error> {
        let FunctionCall {
            function,
            args,
            generics,
            id,
            ..
        } = expr;
        self.write_str(&function.signature.name)?;
        if !generics.is_empty() {
            self.write_char('<')?;
            for (i, ty) in generics.iter().enumerate() {
                if i > 0 {
                    self.write_str(", ")?;
                }
                write!(self, "T{i}={ty}")?;
            }
            self.write_char('>')?;
        }
        self.write_char('<')?;
        for (i, ty) in function.signature.args_type.iter().enumerate() {
            if i > 0 {
                self.write_str(", ")?;
            }
            write!(self, "{ty}")?;
        }
        self.write_char('>')?;

        let params = id.params();
        if !params.is_empty() {
            self.write_char('(')?;
            for (i, ty) in params.iter().enumerate() {
                if i > 0 {
                    self.write_str(", ")?;
                }
                write!(self, "{ty}")?;
            }
            self.write_char(')')?;
        }

        self.write_char('(')?;
        for (i, arg) in args.iter().enumerate() {
            if i > 0 {
                self.write_str(", ")?;
            }
            visit_expr(arg, self)?;
        }
        self.write_char(')')?;
        Ok(None)
    }

    fn enter_lambda_function_call(
        &mut self,
        expr: &LambdaFunctionCall<I>,
    ) -> Result<Option<Expr<I>>, Self::Error> {
        let LambdaFunctionCall {
            name,
            args,
            lambda_display,
            ..
        } = expr;
        self.write_str(name)?;
        self.write_char('(')?;
        for (i, arg) in args.iter().enumerate() {
            if i > 0 {
                self.write_str(", ")?;
            }
            visit_expr(arg, self)?;
        }
        self.write_str(", ")?;
        self.write_str(lambda_display)?;
        self.write_char(')')?;
        Ok(None)
    }
}

impl<Index: ColumnIndex> Expr<Index> {
    pub fn sql_display(&self) -> String {
        fn write_unary_op<Index: ColumnIndex>(
            op: &str,
            expr: &Expr<Index>,
            precedence: usize,
            min_precedence: usize,
        ) -> String {
            if precedence < min_precedence {
                format!("({op} {})", write_expr(expr, precedence))
            } else {
                format!("{op} {}", write_expr(expr, precedence))
            }
        }

        fn write_binary_op<Index: ColumnIndex>(
            op: &str,
            lhs: &Expr<Index>,
            rhs: &Expr<Index>,
            precedence: usize,
            min_precedence: usize,
        ) -> String {
            if precedence < min_precedence || matches!(op, "AND" | "OR") {
                format!(
                    "({} {op} {})",
                    write_expr(lhs, precedence),
                    write_expr(rhs, precedence)
                )
            } else {
                format!(
                    "{} {op} {}",
                    write_expr(lhs, precedence),
                    write_expr(rhs, precedence)
                )
            }
        }

        #[recursive::recursive]
        fn write_expr<Index: ColumnIndex>(expr: &Expr<Index>, min_precedence: usize) -> String {
            match expr {
                Expr::Constant(Constant { scalar, .. }) => match scalar {
                    s @ Scalar::Binary(_) => format!("from_hex('{s}')::string"),
                    Scalar::Number(NumberScalar::Float32(f)) if f.is_nan() => {
                        "'nan'::Float32".to_string()
                    }
                    Scalar::Number(NumberScalar::Float64(f)) if f.is_nan() => {
                        "'nan'::Float64".to_string()
                    }
                    Scalar::Number(NumberScalar::Float32(f)) if f.is_infinite() => {
                        if *f != f32::NEG_INFINITY {
                            "'inf'::Float32".to_string()
                        } else {
                            "'-inf'::Float32".to_string()
                        }
                    }
                    Scalar::Number(NumberScalar::Float64(f)) if f.is_infinite() => {
                        if *f != f64::NEG_INFINITY {
                            "'inf'::Float64".to_string()
                        } else {
                            "'-inf'::Float64".to_string()
                        }
                    }
                    other => other.as_ref().to_string(),
                },
                Expr::ColumnRef(ColumnRef { display_name, .. }) => display_name.clone(),
                Expr::Cast(Cast {
                    is_try,
                    expr,
                    dest_type,
                    ..
                }) => {
                    if *is_try {
                        format!("TRY_CAST({} AS {dest_type})", expr.sql_display())
                    } else {
                        format!("CAST({} AS {dest_type})", expr.sql_display())
                    }
                }
                Expr::FunctionCall(FunctionCall {
                    function, args, id, ..
                }) => match (function.signature.name.as_str(), args.as_slice()) {
                    ("and", [ref lhs, ref rhs]) => {
                        write_binary_op("AND", lhs, rhs, 10, min_precedence)
                    }
                    ("or", [ref lhs, ref rhs]) => {
                        write_binary_op("OR", lhs, rhs, 5, min_precedence)
                    }
                    ("not", [ref expr]) => write_unary_op("NOT", expr, 15, min_precedence),
                    ("gte", [ref lhs, ref rhs]) => {
                        write_binary_op(">=", lhs, rhs, 20, min_precedence)
                    }
                    ("gt", [ref lhs, ref rhs]) => {
                        write_binary_op(">", lhs, rhs, 20, min_precedence)
                    }
                    ("lte", [ref lhs, ref rhs]) => {
                        write_binary_op("<=", lhs, rhs, 20, min_precedence)
                    }
                    ("lt", [ref lhs, ref rhs]) => {
                        write_binary_op("<", lhs, rhs, 20, min_precedence)
                    }
                    ("eq", [ref lhs, ref rhs]) => {
                        write_binary_op("=", lhs, rhs, 20, min_precedence)
                    }
                    ("noteq", [ref lhs, ref rhs]) => {
                        write_binary_op("<>", lhs, rhs, 20, min_precedence)
                    }
                    ("plus", [ref expr]) => write_unary_op("+", expr, 50, min_precedence),
                    ("minus", [ref expr]) => write_unary_op("-", expr, 50, min_precedence),
                    ("plus", [ref lhs, ref rhs]) => {
                        write_binary_op("+", lhs, rhs, 30, min_precedence)
                    }
                    ("minus", [ref lhs, ref rhs]) => {
                        write_binary_op("-", lhs, rhs, 30, min_precedence)
                    }
                    ("multiply", [ref lhs, ref rhs]) => {
                        write_binary_op("*", lhs, rhs, 40, min_precedence)
                    }
                    ("divide", [ref lhs, ref rhs]) => {
                        write_binary_op("/", lhs, rhs, 40, min_precedence)
                    }
                    ("div", [ref lhs, ref rhs]) => {
                        write_binary_op("DIV", lhs, rhs, 40, min_precedence)
                    }
                    ("modulo", [ref lhs, ref rhs]) => {
                        write_binary_op("%", lhs, rhs, 40, min_precedence)
                    }
                    _ => {
                        let mut s = String::new();
                        s += &function.signature.name;

                        let params = id.params();
                        if !params.is_empty() {
                            s += "(";
                            for (i, ty) in params.iter().enumerate() {
                                if i > 0 {
                                    s += ", ";
                                }
                                s += &ty.to_string();
                            }
                            s += ")";
                        }

                        s += "(";
                        for (i, arg) in args.iter().enumerate() {
                            if i > 0 {
                                s += ", ";
                            }
                            s += &arg.sql_display();
                        }
                        s += ")";
                        s
                    }
                },
                Expr::LambdaFunctionCall(LambdaFunctionCall {
                    name,
                    args,
                    lambda_display,
                    ..
                }) => {
                    let mut s = String::new();
                    s += name;
                    s += "(";
                    for (i, arg) in args.iter().enumerate() {
                        if i > 0 {
                            s += ", ";
                        }
                        s += &arg.sql_display();
                    }
                    s += ", ";
                    s += lambda_display;
                    s += ")";
                    s
                }
            }
        }

        write_expr(self, 0)
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

impl Debug for Function {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.signature)
    }
}

impl Debug for FunctionEval {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            FunctionEval::Scalar { .. } => write!(f, "FunctionEval::Scalar"),
            FunctionEval::SRF { .. } => write!(f, "FunctionEval::SRF"),
        }
    }
}

impl Display for FunctionSignature {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut properties = Vec::new();
        if self.non_deterministic {
            properties.push("non_deterministic");
        }
        if !properties.is_empty() {
            write!(f, "{{{}}}", properties.join(", "))?;
        }
        Ok(())
    }
}

impl Display for NullableDomain<AnyType> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(max) = &self.max {
            write!(f, "{{{:?}..={:?}}}", &self.min, max)
        } else {
            write!(f, "{{{:?}..}}", &self.min)
        }
    }
}

impl Display for NumberDomain {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        with_integer_mapped_type!(|TYPE| match self {
            NumberDomain::TYPE(domain) => write!(f, "{domain}"),
            NumberDomain::Float32(SimpleDomain { min, max }) => write!(f, "{}", SimpleDomain {
                min: display_f32(**min),
                max: display_f32(**max),
            }),
            NumberDomain::Float64(SimpleDomain { min, max }) => write!(f, "{}", SimpleDomain {
                min: display_f64(**min),
                max: display_f64(**max),
            }),
        })
    }
}

impl Display for DecimalDomain {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            DecimalDomain::Decimal64(SimpleDomain { min, max }, size) => {
                write!(f, "{}", SimpleDomain {
                    min: display_decimal_128(*min as i128, size.scale()),
                    max: display_decimal_128(*max as i128, size.scale()),
                })
            }
            DecimalDomain::Decimal128(SimpleDomain { min, max }, size) => {
                write!(f, "{}", SimpleDomain {
                    min: display_decimal_128(*min, size.scale()),
                    max: display_decimal_128(*max, size.scale()),
                })
            }
            DecimalDomain::Decimal256(SimpleDomain { min, max }, size) => {
                write!(f, "{}", SimpleDomain {
                    min: display_decimal_256(min.0, size.scale()),
                    max: display_decimal_256(max.0, size.scale()),
                })
            }
        }
    }
}

impl<T: Display> Display for SimpleDomain<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{{{}..={}}}", self.min, self.max)
    }
}

impl Display for Domain {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Domain::Number(domain) => write!(f, "{domain}"),
            Domain::Decimal(domain) => write!(f, "{domain}"),
            Domain::Boolean(domain) => write!(f, "{domain}"),
            Domain::String(domain) => write!(f, "{domain}"),
            Domain::Timestamp(domain) => write!(f, "{domain}"),
            Domain::TimestampTimezone(domain) => write!(f, "{domain}"),
            Domain::Date(domain) => write!(f, "{domain}"),
            Domain::Interval(domain) => write!(f, "{:?}", domain),
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
            Domain::Map(None) => write!(f, "{{}}"),
            Domain::Map(Some(domain)) => {
                let inner_domain = domain.as_tuple().unwrap();
                let key_domain = &inner_domain[0];
                let val_domain = &inner_domain[1];
                write!(f, "{{[{key_domain}], [{val_domain}]}}")
            }
            Domain::Undefined => write!(f, "Undefined"),
        }
    }
}

/// Display a float as with fixed number of fractional digits to avoid test failures due to
/// rounding differences between MacOS and Linus.
fn display_f32(num: f32) -> String {
    match Decimal::from_f32(num) {
        Some(d) => d
            .round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
            .normalize()
            .to_string(),
        None => num.to_string(),
    }
}

/// Display a float as with fixed number of fractional digits to avoid test failures due to
/// rounding differences between MacOS and Linus.
fn display_f64(num: f64) -> String {
    match Decimal::from_f64(num) {
        Some(d) => d
            .round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
            .normalize()
            .to_string(),
        None => num.to_string(),
    }
}

/// Display a tuple field name, if it contains uppercase letters or special characters, add quotes.
pub fn display_tuple_field_name(field_name: &str) -> String {
    display_ident(field_name, false, true, Dialect::PostgreSQL)
}
