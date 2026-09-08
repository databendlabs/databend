// Copyright 2026 Databend Labs
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

//! Fixed historical data. This module never resolves an aggregate implementation.
use databend_common_ast::ast::Expr;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_expression::Scalar;
use databend_common_expression::types::AggregateStateDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::i256;

pub struct PreparedCall {
    pub expression: &'static str,
    pub arguments: Vec<DataType>,
    pub state: DataType,
}

pub struct Sample {
    pub label: &'static str,
    pub inputs: Vec<databend_common_expression::Column>,
    pub state: Scalar,
    pub result: Scalar,
    pub merge_result: MergeResult,
}

pub enum MergeResult {
    Skip,
    SameAsResult,
    Value(Scalar),
}

/// Every case checks metadata; Samples additionally checks historical state behavior.
pub enum Case {
    Metadata {
        expression: &'static str,
        arguments: Vec<&'static str>,
        result: &'static str,
        state: &'static str,
    },
    Samples {
        expression: &'static str,
        arguments: Vec<&'static str>,
        result: &'static str,
        state: &'static str,
        samples: Vec<Sample>,
    },
}

impl PreparedCall {
    pub fn new(
        expression: &'static str,
        arguments: Vec<&'static str>,
        state: &'static str,
    ) -> Self {
        Self {
            expression,
            arguments: arguments.into_iter().map(data_type).collect(),
            state: data_type(state),
        }
    }

    /// Parse the call without resolving it against the implementation under test.
    /// This stage permits named column arguments only; reject unsupported syntax
    /// before a RawExpr conversion could discard aggregate modifiers.
    pub fn name(&self) -> String {
        let tokens = tokenize_sql(self.expression).unwrap();
        let Expr::FunctionCall { func, .. } = parse_expr(&tokens, Dialect::PostgreSQL).unwrap()
        else {
            panic!("expected aggregate call: {}", self.expression)
        };
        assert!(
            !func.distinct
                && func.params.is_empty()
                && func.order_by.is_empty()
                && func.filter.is_none()
                && func.window.is_none()
                && func.lambda.is_none(),
            "unsupported aggregate modifiers: {}",
            self.expression
        );
        assert_eq!(func.args.len(), self.arguments.len());
        for (index, arg) in func.args.iter().enumerate() {
            let Expr::ColumnRef { column, .. } = arg else {
                panic!("expected named input: {arg}")
            };
            assert!(column.database.is_none() && column.table.is_none());
            assert_eq!(column.column.name(), format!("x{index}"));
        }
        func.name.name
    }

    pub fn state_type(&self) -> DataType {
        DataType::AggregateState(Box::new(AggregateStateDataType {
            function_name: self.name(),
            params: vec![],
            argument_types: self.arguments.clone(),
            state_type: Box::new(self.state.clone()),
        }))
    }
}

pub fn data_type(name: &str) -> DataType {
    // Preserve explicit nullability, including inside arrays and tuples.
    let table_type = databend_common_expression::resolve_type_name_by_str(name, true)
        .unwrap_or_else(|error| panic!("invalid baseline type {name:?}: {error}"));
    DataType::from(&table_type)
}

pub fn tuple(fields: Vec<Scalar>) -> Scalar {
    Scalar::Tuple(fields)
}
pub fn int64(value: i64) -> Scalar {
    Scalar::Number(NumberScalar::Int64(value))
}
pub fn uint64(value: u64) -> Scalar {
    Scalar::Number(NumberScalar::UInt64(value))
}
pub fn float64(value: f64) -> Scalar {
    Scalar::Number(NumberScalar::Float64(value.into()))
}
pub fn string(value: &str) -> Scalar {
    Scalar::String(value.to_owned())
}
pub fn decimal64(value: i64, precision: u8, scale: u8) -> Scalar {
    Scalar::Decimal(DecimalScalar::Decimal64(
        value,
        DecimalSize::new(precision, scale).unwrap(),
    ))
}
pub fn decimal128(value: i128, precision: u8, scale: u8) -> Scalar {
    Scalar::Decimal(DecimalScalar::Decimal128(
        value,
        DecimalSize::new(precision, scale).unwrap(),
    ))
}
pub fn decimal256(value: i256, precision: u8, scale: u8) -> Scalar {
    Scalar::Decimal(DecimalScalar::Decimal256(
        value,
        DecimalSize::new(precision, scale).unwrap(),
    ))
}

pub fn bytes(encoded: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .unwrap()
}
pub fn binary(encoded: &str) -> Scalar {
    Scalar::Binary(bytes(encoded))
}
pub fn geometry(encoded: &str) -> Scalar {
    Scalar::Geometry(bytes(encoded))
}
pub fn variant(encoded: &str) -> Scalar {
    Scalar::Variant(bytes(encoded))
}

#[test]
fn call_expression_rejects_unsupported_syntax() {
    for expression in [
        "sum(DISTINCT x0)",
        "sum(x0) FILTER (WHERE true)",
        "sum(x0 ORDER BY x0)",
        "sum(x0) OVER ()",
        "quantile_cont(0.5)(x0)",
        "sum(x1)",
        "sum(x0::Int64)",
    ] {
        let call = PreparedCall::new(expression, vec!["Int64"], "Boolean");
        assert!(
            databend_common_base::runtime::catch_unwind(|| call.name()).is_err(),
            "silently accepted {expression}"
        );
    }
    let call = PreparedCall::new("uniq(x0, x1)", vec!["String", "Boolean"], "Boolean");
    assert_eq!(call.name(), "uniq");
}
