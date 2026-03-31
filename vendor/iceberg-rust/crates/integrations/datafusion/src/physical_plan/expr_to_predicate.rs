// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::vec;

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression};
use iceberg::spec::Datum;

// A datafusion expression could be an Iceberg predicate, column, or literal.
enum TransformedResult {
    Predicate(Predicate),
    Column(Reference),
    Literal(Datum),
    NotTransformed,
}

enum OpTransformedResult {
    Operator(PredicateOperator),
    And,
    Or,
    NotTransformed,
}

/// Converts DataFusion filters ([`Expr`]) to an iceberg [`Predicate`].
/// If none of the filters could be converted, return `None` which adds no predicates to the scan operation.
/// If the conversion was successful, return the converted predicates combined with an AND operator.
pub fn convert_filters_to_predicate(filters: &[Expr]) -> Option<Predicate> {
    filters
        .iter()
        .filter_map(convert_filter_to_predicate)
        .reduce(Predicate::and)
}

fn convert_filter_to_predicate(expr: &Expr) -> Option<Predicate> {
    match to_iceberg_predicate(expr) {
        TransformedResult::Predicate(predicate) => Some(predicate),
        TransformedResult::Column(_) | TransformedResult::Literal(_) => {
            unreachable!("Not a valid expression: {:?}", expr)
        }
        _ => None,
    }
}

fn to_iceberg_predicate(expr: &Expr) -> TransformedResult {
    match expr {
        Expr::BinaryExpr(binary) => {
            let left = to_iceberg_predicate(&binary.left);
            let right = to_iceberg_predicate(&binary.right);
            let op = to_iceberg_operation(binary.op);
            match op {
                OpTransformedResult::Operator(op) => to_iceberg_binary_predicate(left, right, op),
                OpTransformedResult::And => to_iceberg_and_predicate(left, right),
                OpTransformedResult::Or => to_iceberg_or_predicate(left, right),
                OpTransformedResult::NotTransformed => TransformedResult::NotTransformed,
            }
        }
        Expr::Not(exp) => {
            let expr = to_iceberg_predicate(exp);
            match expr {
                TransformedResult::Predicate(p) => TransformedResult::Predicate(!p),
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::Column(column) => TransformedResult::Column(Reference::new(column.name())),
        Expr::Literal(literal, _) => match scalar_value_to_datum(literal) {
            Some(data) => TransformedResult::Literal(data),
            None => TransformedResult::NotTransformed,
        },
        Expr::InList(inlist) => {
            let mut datums = vec![];
            for expr in &inlist.list {
                let p = to_iceberg_predicate(expr);
                match p {
                    TransformedResult::Literal(l) => datums.push(l),
                    _ => return TransformedResult::NotTransformed,
                }
            }

            let expr = to_iceberg_predicate(&inlist.expr);
            match expr {
                TransformedResult::Column(r) => match inlist.negated {
                    false => TransformedResult::Predicate(r.is_in(datums)),
                    true => TransformedResult::Predicate(r.is_not_in(datums)),
                },
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::IsNull(expr) => {
            let p = to_iceberg_predicate(expr);
            match p {
                TransformedResult::Column(r) => TransformedResult::Predicate(Predicate::Unary(
                    UnaryExpression::new(PredicateOperator::IsNull, r),
                )),
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::IsNotNull(expr) => {
            let p = to_iceberg_predicate(expr);
            match p {
                TransformedResult::Column(r) => TransformedResult::Predicate(Predicate::Unary(
                    UnaryExpression::new(PredicateOperator::NotNull, r),
                )),
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::Cast(c) => {
            if c.data_type == DataType::Date32 || c.data_type == DataType::Date64 {
                // Casts to date truncate the expression, we cannot simply extract it as it
                // can create erroneous predicates.
                return TransformedResult::NotTransformed;
            }
            to_iceberg_predicate(&c.expr)
        }
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_operation(op: Operator) -> OpTransformedResult {
    match op {
        Operator::Eq => OpTransformedResult::Operator(PredicateOperator::Eq),
        Operator::NotEq => OpTransformedResult::Operator(PredicateOperator::NotEq),
        Operator::Lt => OpTransformedResult::Operator(PredicateOperator::LessThan),
        Operator::LtEq => OpTransformedResult::Operator(PredicateOperator::LessThanOrEq),
        Operator::Gt => OpTransformedResult::Operator(PredicateOperator::GreaterThan),
        Operator::GtEq => OpTransformedResult::Operator(PredicateOperator::GreaterThanOrEq),
        // AND OR
        Operator::And => OpTransformedResult::And,
        Operator::Or => OpTransformedResult::Or,
        // Others not supported
        _ => OpTransformedResult::NotTransformed,
    }
}

fn to_iceberg_and_predicate(
    left: TransformedResult,
    right: TransformedResult,
) -> TransformedResult {
    match (left, right) {
        (TransformedResult::Predicate(left), TransformedResult::Predicate(right)) => {
            TransformedResult::Predicate(left.and(right))
        }
        (TransformedResult::Predicate(left), _) => TransformedResult::Predicate(left),
        (_, TransformedResult::Predicate(right)) => TransformedResult::Predicate(right),
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_or_predicate(left: TransformedResult, right: TransformedResult) -> TransformedResult {
    match (left, right) {
        (TransformedResult::Predicate(left), TransformedResult::Predicate(right)) => {
            TransformedResult::Predicate(left.or(right))
        }
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_binary_predicate(
    left: TransformedResult,
    right: TransformedResult,
    op: PredicateOperator,
) -> TransformedResult {
    let (r, d, op) = match (left, right) {
        (TransformedResult::NotTransformed, _) => return TransformedResult::NotTransformed,
        (_, TransformedResult::NotTransformed) => return TransformedResult::NotTransformed,
        (TransformedResult::Column(r), TransformedResult::Literal(d)) => (r, d, op),
        (TransformedResult::Literal(d), TransformedResult::Column(r)) => {
            (r, d, reverse_predicate_operator(op))
        }
        _ => return TransformedResult::NotTransformed,
    };
    TransformedResult::Predicate(Predicate::Binary(BinaryExpression::new(op, r, d)))
}

fn reverse_predicate_operator(op: PredicateOperator) -> PredicateOperator {
    match op {
        PredicateOperator::Eq => PredicateOperator::Eq,
        PredicateOperator::NotEq => PredicateOperator::NotEq,
        PredicateOperator::GreaterThan => PredicateOperator::LessThan,
        PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
        PredicateOperator::LessThan => PredicateOperator::GreaterThan,
        PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
        _ => unreachable!("Reverse {}", op),
    }
}

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;
/// Convert a scalar value to an iceberg datum.
fn scalar_value_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int16(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::Float32(Some(v)) => Some(Datum::double(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(Datum::double(*v)),
        ScalarValue::Utf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::Date32(Some(v)) => Some(Datum::date(*v)),
        ScalarValue::Date64(Some(v)) => Some(Datum::date((*v / MILLIS_PER_DAY) as i32)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::utils::split_conjunction;
    use datafusion::prelude::{Expr, SessionContext};
    use iceberg::expr::{Predicate, Reference};
    use iceberg::spec::Datum;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::convert_filters_to_predicate;

    fn create_test_schema() -> DFSchema {
        let arrow_schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("bar", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())]),
            ),
        ]);
        DFSchema::try_from_qualified_schema("my_table", &arrow_schema).unwrap()
    }

    fn convert_to_iceberg_predicate(sql: &str) -> Option<Predicate> {
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let exprs: Vec<Expr> = split_conjunction(&expr).into_iter().cloned().collect();
        convert_filters_to_predicate(&exprs[..])
    }

    #[test]
    fn test_predicate_conversion_with_single_condition() {
        let predicate = convert_to_iceberg_predicate("foo = 1").unwrap();
        assert_eq!(predicate, Reference::new("foo").equal_to(Datum::long(1)));

        let predicate = convert_to_iceberg_predicate("foo != 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").not_equal_to(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo > 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo >= 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than_or_equal_to(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo < 1").unwrap();
        assert_eq!(predicate, Reference::new("foo").less_than(Datum::long(1)));

        let predicate = convert_to_iceberg_predicate("foo <= 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").less_than_or_equal_to(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo is null").unwrap();
        assert_eq!(predicate, Reference::new("foo").is_null());

        let predicate = convert_to_iceberg_predicate("foo is not null").unwrap();
        assert_eq!(predicate, Reference::new("foo").is_not_null());

        let predicate = convert_to_iceberg_predicate("foo in (5, 6)").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").is_in([Datum::long(5), Datum::long(6)])
        );

        let predicate = convert_to_iceberg_predicate("foo not in (5, 6)").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").is_not_in([Datum::long(5), Datum::long(6)])
        );

        let predicate = convert_to_iceberg_predicate("not foo = 1").unwrap();
        assert_eq!(predicate, !Reference::new("foo").equal_to(Datum::long(1)));
    }

    #[test]
    fn test_predicate_conversion_with_single_unsupported_condition() {
        let predicate = convert_to_iceberg_predicate("foo + 1 = 1");
        assert_eq!(predicate, None);

        let predicate = convert_to_iceberg_predicate("length(bar) = 1");
        assert_eq!(predicate, None);

        let predicate = convert_to_iceberg_predicate("foo in (1, 2, foo)");
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_single_condition_rev() {
        let predicate = convert_to_iceberg_predicate("1 < foo").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than(Datum::long(1))
        );
    }

    #[test]
    fn test_predicate_conversion_with_and_condition() {
        let sql = "foo > 1 and bar = 'test'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_and_condition_unsupported() {
        let sql = "foo > 1 and length(bar) = 1";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Reference::new("foo").greater_than(Datum::long(1));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_and_condition_both_unsupported() {
        let sql = "foo in (1, 2, foo) and length(bar) = 1";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_or_condition_unsupported() {
        let sql = "foo > 1 or length(bar) = 1";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_or_condition_supported() {
        let sql = "foo > 1 or bar = 'test'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::or(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_complex_binary_expr() {
        let sql = "(foo > 1 and bar = 'test') or foo < 0 ";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();

        let inner_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        let expected_predicate = Predicate::or(
            inner_predicate,
            Reference::new("foo").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_one_and_expr_supported() {
        let sql = "(foo > 1 and length(bar) = 1 ) or foo < 0 ";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();

        let inner_predicate = Reference::new("foo").greater_than(Datum::long(1));
        let expected_predicate = Predicate::or(
            inner_predicate,
            Reference::new("foo").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_complex_binary_expr_unsupported() {
        let sql = "(foo > 1 or length(bar) = 1 ) and foo < 0 ";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Reference::new("foo").less_than(Datum::long(0));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_cast() {
        let sql = "ts >= timestamp '2023-01-05T00:00:00'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate =
            Reference::new("ts").greater_than_or_equal_to(Datum::string("2023-01-05T00:00:00"));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_date_cast() {
        let sql = "ts >= date '2023-01-05T11:00:00'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }
}
