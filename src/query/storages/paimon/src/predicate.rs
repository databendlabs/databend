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

use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::NumberScalar;
use paimon::ReadBuilder;
use paimon::spec::DataField;
use paimon::spec::DataType as PaimonDataType;
use paimon::spec::Datum;
use paimon::spec::Predicate;
use paimon::spec::PredicateBuilder;

pub struct PaimonPredicateAnalysis {
    pub predicate: Option<Predicate>,
    pub has_residual: bool,
}

pub fn can_push_limit(
    limit: Option<usize>,
    analysis: &PaimonPredicateAnalysis,
    read_builder: &ReadBuilder<'_>,
) -> bool {
    limit.is_some()
        && !analysis.has_residual
        && analysis
            .predicate
            .as_ref()
            .is_none_or(|p| read_builder.is_exact_filter_pushdown(p))
}

pub fn analyze_predicate(
    expr: &RemoteExpr<String>,
    fields: &[DataField],
) -> PaimonPredicateAnalysis {
    let builder = PredicateBuilder::new(fields);
    let mut conjuncts = Vec::new();
    let mut has_residual = false;

    for conjunct in split_conjuncts(expr) {
        match translate_expr(&conjunct, &builder, fields) {
            TranslateResult::Ok(pred) => conjuncts.push(pred),
            TranslateResult::Reject => has_residual = true,
        }
    }

    let predicate = if conjuncts.is_empty() {
        None
    } else {
        Some(Predicate::and(conjuncts))
    };

    PaimonPredicateAnalysis {
        predicate,
        has_residual,
    }
}

enum TranslateResult {
    Ok(Predicate),
    Reject,
}

fn split_conjuncts(expr: &RemoteExpr<String>) -> Vec<RemoteExpr<String>> {
    match expr {
        RemoteExpr::FunctionCall { id, args, .. }
            if matches!(id.name().as_ref(), "and" | "and_filters") && args.len() == 2 =>
        {
            let mut conjuncts = split_conjuncts(&args[0]);
            conjuncts.extend(split_conjuncts(&args[1]));
            conjuncts
        }
        other => vec![other.clone()],
    }
}

fn translate_expr(
    expr: &RemoteExpr<String>,
    builder: &PredicateBuilder,
    fields: &[DataField],
) -> TranslateResult {
    match expr {
        RemoteExpr::FunctionCall { id, args, .. }
            if (id.name().as_ref() == "or" || id.name().as_ref() == "or_filters")
                && args.len() == 2 =>
        {
            let left = translate_expr(&args[0], builder, fields);
            let right = translate_expr(&args[1], builder, fields);
            match (left, right) {
                (TranslateResult::Ok(left), TranslateResult::Ok(right)) => {
                    TranslateResult::Ok(Predicate::or(vec![left, right]))
                }
                _ => TranslateResult::Reject,
            }
        }
        RemoteExpr::FunctionCall { id, args, .. }
            if args.len() == 1 && matches!(args[0], RemoteExpr::ColumnRef { .. }) =>
        {
            let (_, name, _, _) = args[0].as_column_ref().unwrap();
            match id.name().as_ref() {
                "is_null" => match builder.is_null(name) {
                    Ok(pred) => TranslateResult::Ok(pred),
                    Err(_) => TranslateResult::Reject,
                },
                "is_not_null" => match builder.is_not_null(name) {
                    Ok(pred) => TranslateResult::Ok(pred),
                    Err(_) => TranslateResult::Reject,
                },
                _ => TranslateResult::Reject,
            }
        }
        RemoteExpr::FunctionCall { id, args, .. }
            if args.len() == 2
                && matches!(args[0], RemoteExpr::ColumnRef { .. })
                && matches!(args[1], RemoteExpr::Constant { .. }) =>
        {
            let (_, constant_scalar, constant_type) = args[1].as_constant().unwrap();
            translate_binary(
                builder,
                fields,
                args[0].as_column_ref().unwrap().1,
                id.name().as_ref(),
                (constant_scalar, constant_type),
            )
        }
        RemoteExpr::FunctionCall { id, args, .. }
            if args.len() == 2
                && matches!(args[1], RemoteExpr::ColumnRef { .. })
                && matches!(args[0], RemoteExpr::Constant { .. }) =>
        {
            let op = match id.name().as_ref() {
                "eq" => "eq",
                "noteq" => "noteq",
                "lt" => "gt",
                "lte" => "gte",
                "gt" => "lt",
                "gte" => "lte",
                _ => return TranslateResult::Reject,
            };
            let (_, constant_scalar, constant_type) = args[0].as_constant().unwrap();
            translate_binary(
                builder,
                fields,
                args[1].as_column_ref().unwrap().1,
                op,
                (constant_scalar, constant_type),
            )
        }
        RemoteExpr::FunctionCall { id, args, .. }
            if matches!(id.name().as_ref(), "inlist" | "in" | "not_inlist") && args.len() >= 2 =>
        {
            let (_, name, _, _) = match &args[0] {
                RemoteExpr::ColumnRef { .. } => args[0].as_column_ref().unwrap(),
                _ => return TranslateResult::Reject,
            };
            let field_type = fields
                .iter()
                .find(|field| field.name() == name)
                .map(|field| field.data_type());
            let mut literals = Vec::with_capacity(args.len() - 1);
            for arg in &args[1..] {
                let RemoteExpr::Constant {
                    scalar, data_type, ..
                } = arg
                else {
                    return TranslateResult::Reject;
                };
                let datum = match scalar_to_datum(scalar, field_type, data_type) {
                    Some(datum) => datum,
                    None => return TranslateResult::Reject,
                };
                literals.push(datum);
            }
            let pred = match id.name().as_ref() {
                "not_inlist" => builder.is_not_in(name, literals),
                _ => builder.is_in(name, literals),
            };
            match pred {
                Ok(pred) => TranslateResult::Ok(pred),
                Err(_) => TranslateResult::Reject,
            }
        }
        RemoteExpr::FunctionCall { id, args, .. } if id.name().as_ref() == "between" => {
            if args.len() != 3 {
                return TranslateResult::Reject;
            }
            let (_, name, _, _) = match &args[0] {
                RemoteExpr::ColumnRef { .. } => args[0].as_column_ref().unwrap(),
                _ => return TranslateResult::Reject,
            };
            let field_type = fields
                .iter()
                .find(|field| field.name() == name)
                .map(|field| field.data_type());
            let RemoteExpr::Constant {
                scalar: low_scalar,
                data_type: low_type,
                ..
            } = &args[1]
            else {
                return TranslateResult::Reject;
            };
            let RemoteExpr::Constant {
                scalar: high_scalar,
                data_type: high_type,
                ..
            } = &args[2]
            else {
                return TranslateResult::Reject;
            };
            let low = match scalar_to_datum(low_scalar, field_type, low_type) {
                Some(datum) => datum,
                None => return TranslateResult::Reject,
            };
            let high = match scalar_to_datum(high_scalar, field_type, high_type) {
                Some(datum) => datum,
                None => return TranslateResult::Reject,
            };
            let lower = match builder.greater_or_equal(name, low) {
                Ok(pred) => pred,
                Err(_) => return TranslateResult::Reject,
            };
            let upper = match builder.less_or_equal(name, high) {
                Ok(pred) => pred,
                Err(_) => return TranslateResult::Reject,
            };
            TranslateResult::Ok(Predicate::and(vec![lower, upper]))
        }
        RemoteExpr::FunctionCall { id, .. } if id.name().as_ref() == "not" => {
            TranslateResult::Reject
        }
        _ => TranslateResult::Reject,
    }
}

fn translate_binary(
    builder: &PredicateBuilder,
    fields: &[DataField],
    name: &str,
    op: &str,
    constant: (&Scalar, &DataType),
) -> TranslateResult {
    let field_type = fields
        .iter()
        .find(|field| field.name() == name)
        .map(|field| field.data_type());
    let datum = match scalar_to_datum(constant.0, field_type, constant.1) {
        Some(datum) => datum,
        None => return TranslateResult::Reject,
    };
    let pred = match op {
        "eq" => builder.equal(name, datum),
        "noteq" => builder.not_equal(name, datum),
        "lt" => builder.less_than(name, datum),
        "lte" => builder.less_or_equal(name, datum),
        "gt" => builder.greater_than(name, datum),
        "gte" => builder.greater_or_equal(name, datum),
        _ => return TranslateResult::Reject,
    };
    match pred {
        Ok(pred) => TranslateResult::Ok(pred),
        Err(_) => TranslateResult::Reject,
    }
}

fn scalar_to_datum(
    scalar: &Scalar,
    field_type: Option<&PaimonDataType>,
    _expr_type: &DataType,
) -> Option<Datum> {
    if let Some(field_type) = field_type {
        return scalar_to_datum_for_type(scalar, field_type);
    }
    match scalar {
        Scalar::Boolean(v) => Some(Datum::Bool(*v)),
        Scalar::String(v) => Some(Datum::String(v.clone())),
        Scalar::Number(n) => match n {
            NumberScalar::Int8(v) => Some(Datum::TinyInt(*v)),
            NumberScalar::Int16(v) => Some(Datum::SmallInt(*v)),
            NumberScalar::Int32(v) => Some(Datum::Int(*v)),
            NumberScalar::Int64(v) => Some(Datum::Long(*v)),
            NumberScalar::Float32(v) => Some(Datum::Float(v.0)),
            NumberScalar::Float64(v) => Some(Datum::Double(v.0)),
            _ => None,
        },
        Scalar::Date(v) => Some(Datum::Date(*v)),
        Scalar::Timestamp(v) => Some(databend_timestamp_micros_to_datum(*v, false)),
        Scalar::Decimal(dec) => {
            let value = decimal_scalar_to_i128(dec)?;
            let size = dec.size();
            Some(Datum::Decimal {
                unscaled: value,
                precision: size.precision() as u32,
                scale: size.scale() as u32,
            })
        }
        Scalar::Binary(v) => Some(Datum::Bytes(v.clone())),
        _ => None,
    }
}

fn scalar_to_datum_for_type(scalar: &Scalar, field_type: &PaimonDataType) -> Option<Datum> {
    match field_type {
        PaimonDataType::Boolean(_) => match scalar {
            Scalar::Boolean(v) => Some(Datum::Bool(*v)),
            _ => None,
        },
        PaimonDataType::TinyInt(_) => match scalar {
            Scalar::Number(NumberScalar::Int8(v)) => Some(Datum::TinyInt(*v)),
            _ => None,
        },
        PaimonDataType::SmallInt(_) => match scalar {
            Scalar::Number(NumberScalar::Int16(v)) => Some(Datum::SmallInt(*v)),
            _ => None,
        },
        PaimonDataType::Int(_) => match scalar {
            Scalar::Number(NumberScalar::Int32(v)) => Some(Datum::Int(*v)),
            _ => None,
        },
        PaimonDataType::BigInt(_) => match scalar {
            Scalar::Number(NumberScalar::Int64(v)) => Some(Datum::Long(*v)),
            _ => None,
        },
        PaimonDataType::Float(_) => match scalar {
            Scalar::Number(NumberScalar::Float32(v)) => Some(Datum::Float(v.0)),
            _ => None,
        },
        PaimonDataType::Double(_) => match scalar {
            Scalar::Number(NumberScalar::Float64(v)) => Some(Datum::Double(v.0)),
            _ => None,
        },
        PaimonDataType::VarChar(_) | PaimonDataType::Char(_) => match scalar {
            Scalar::String(v) => Some(Datum::String(v.clone())),
            _ => None,
        },
        PaimonDataType::Date(_) => match scalar {
            Scalar::Date(v) => Some(Datum::Date(*v)),
            _ => None,
        },
        PaimonDataType::Timestamp(_) => match scalar {
            Scalar::Timestamp(v) => Some(databend_timestamp_micros_to_datum(*v, false)),
            _ => None,
        },
        PaimonDataType::LocalZonedTimestamp(_) => match scalar {
            Scalar::Timestamp(v) => Some(databend_timestamp_micros_to_datum(*v, true)),
            _ => None,
        },
        PaimonDataType::Decimal(decimal_type) => {
            let dec = match scalar {
                Scalar::Decimal(dec) => dec,
                _ => return None,
            };
            let value = decimal_scalar_to_i128(dec)?;
            let precision = dec.size().precision() as u32;
            let scale = dec.size().scale() as u32;
            if decimal_type.precision() != precision || decimal_type.scale() != scale {
                return None;
            }
            Some(Datum::Decimal {
                unscaled: value,
                precision: decimal_type.precision(),
                scale: decimal_type.scale(),
            })
        }
        PaimonDataType::Binary(_) | PaimonDataType::VarBinary(_) | PaimonDataType::Blob(_) => {
            match scalar {
                Scalar::Binary(v) => Some(Datum::Bytes(v.clone())),
                _ => None,
            }
        }
        _ => None,
    }
}

fn databend_timestamp_micros_to_datum(micros: i64, local_zoned: bool) -> Datum {
    const MICROS_PER_MILLI: i64 = 1_000;
    const NANOS_PER_MICRO: i64 = 1_000;
    let millis = micros.div_euclid(MICROS_PER_MILLI);
    let sub_milli_micros = micros.rem_euclid(MICROS_PER_MILLI);
    let nanos = (sub_milli_micros * NANOS_PER_MICRO) as i32;
    if local_zoned {
        Datum::LocalZonedTimestamp { millis, nanos }
    } else {
        Datum::Timestamp { millis, nanos }
    }
}

fn decimal_scalar_to_i128(dec: &DecimalScalar) -> Option<i128> {
    match dec {
        DecimalScalar::Decimal64(v, _) => Some(*v as i128),
        DecimalScalar::Decimal128(v, _) => Some(*v),
        DecimalScalar::Decimal256(v, _) => Some(v.as_i128()),
    }
}

pub fn projection_column_names(
    projection: &databend_common_catalog::plan::Projection,
    schema: &databend_common_expression::TableSchema,
) -> Vec<String> {
    projection
        .project_schema(schema)
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

pub fn apply_pushdowns<'a>(
    table: &'a paimon::Table,
    push_downs: Option<&databend_common_catalog::plan::PushDownInfo>,
    databend_schema: &databend_common_expression::TableSchema,
) -> (ReadBuilder<'a>, PaimonPredicateAnalysis) {
    let mut read_builder = table.new_read_builder();
    let analysis = if let Some(push_downs) = push_downs {
        if let Some(projection) = &push_downs.projection {
            let columns = projection_column_names(projection, databend_schema);
            let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
            read_builder.with_projection(&column_refs);
        }
        let analysis = push_downs
            .filters
            .as_ref()
            .map(|filters| analyze_predicate(&filters.filter, table.schema().fields()))
            .unwrap_or(PaimonPredicateAnalysis {
                predicate: None,
                has_residual: false,
            });
        if let Some(predicate) = &analysis.predicate {
            read_builder.with_filter(predicate.clone());
        }
        if can_push_limit(push_downs.limit, &analysis, &read_builder)
            && let Some(limit) = push_downs.limit
        {
            read_builder.with_limit(limit);
        }
        analysis
    } else {
        PaimonPredicateAnalysis {
            predicate: None,
            has_residual: false,
        }
    };
    (read_builder, analysis)
}

#[cfg(test)]
mod unit_tests {
    use paimon::spec::Datum;

    use super::*;

    #[test]
    fn timestamp_positive_and_negative_micros() {
        assert_eq!(
            databend_timestamp_micros_to_datum(1_500_000, false),
            Datum::Timestamp {
                millis: 1_500,
                nanos: 0
            }
        );
        assert_eq!(
            databend_timestamp_micros_to_datum(-1_500_000, false),
            Datum::Timestamp {
                millis: -1_500,
                nanos: 0
            }
        );
    }

    #[test]
    fn timestamp_submillis_and_local_zoned() {
        assert_eq!(
            databend_timestamp_micros_to_datum(1_500, false),
            Datum::Timestamp {
                millis: 1,
                nanos: 500_000
            }
        );
        assert_eq!(
            databend_timestamp_micros_to_datum(-500, false),
            Datum::Timestamp {
                millis: -1,
                nanos: 500_000
            }
        );
        assert_eq!(
            databend_timestamp_micros_to_datum(1_500, true),
            Datum::LocalZonedTimestamp {
                millis: 1,
                nanos: 500_000
            }
        );
    }
}
