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

//! Shared support for runtime tests: exact BigInt semantics, edge values,
//! column construction and result inspection. SQL-visible behavior belongs
//! to the golden files of `databend-common-functions`.

use std::sync::Arc;

use databend_common_expression::Column;
use databend_common_expression::EvalContext;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::type_check;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_integer_mapped_type;
use num_bigint::BigInt;
use num_traits::Signed;

use crate::decimal::DecimalExt;

pub const INTEGERS: [NumberDataType; 8] = [
    NumberDataType::Int8,
    NumberDataType::Int16,
    NumberDataType::Int32,
    NumberDataType::Int64,
    NumberDataType::UInt8,
    NumberDataType::UInt16,
    NumberDataType::UInt32,
    NumberDataType::UInt64,
];

pub fn decimal_type(precision: u8, scale: u8) -> DataType {
    DataType::Decimal(DecimalSize::new(precision, scale).unwrap())
}

/// The given scales that precision `p` admits, ascending and distinct.
pub fn scales<const N: usize>(p: u8, scales: [u8; N]) -> impl Iterator<Item = u8> {
    let mut scales = scales.to_vec();
    scales.retain(|&s| s <= p);
    scales.sort_unstable();
    scales.dedup();
    scales.into_iter()
}

/// Decimal types with scales {0, 1, p/2, p-1, p} for each precision, then
/// every integer type.
pub fn operand_types(precisions: impl IntoIterator<Item = u8>) -> Vec<DataType> {
    let mut types = vec![];
    for p in precisions {
        types.extend(scales(p, [0, 1, p / 2, p - 1, p]).map(|s| decimal_type(p, s)));
    }
    types.extend(INTEGERS.map(DataType::Number));
    types
}

/// Operand pairs a decimal binary signature accepts.
pub fn signatures(types: &[DataType]) -> impl Iterator<Item = [&DataType; 2]> {
    types.iter().flat_map(move |x| {
        types
            .iter()
            .filter(move |y| x.is_decimal() || y.is_decimal())
            .map(move |y| [x, y])
    })
}

pub fn pow10(exponent: u32) -> BigInt {
    BigInt::from(10).pow(exponent)
}

pub fn fits(value: &BigInt, precision: u8) -> bool {
    value.abs() < pow10(precision as u32)
}

/// Quotient rounded half away from zero.
pub fn round_half_away(numerator: &BigInt, divisor: &BigInt) -> BigInt {
    let zero = BigInt::from(0);
    let negative = (numerator < &zero) != (divisor < &zero);
    let (n, d) = (numerator.abs(), divisor.abs());
    let quotient = &n / &d;
    let quotient = if (n % &d) * 2 >= d {
        quotient + 1_u8
    } else {
        quotient
    };
    if negative { -quotient } else { quotient }
}

/// Logical value range of an operand type, as decimal coefficients.
fn range(data_type: &DataType) -> (BigInt, BigInt) {
    match data_type {
        DataType::Decimal(size) => {
            let max = pow10(size.precision() as u32) - 1_u8;
            (-max.clone(), max)
        }
        DataType::Number(ty) => with_integer_mapped_type!(|NUM| match ty {
            NumberDataType::NUM => (BigInt::from(NUM::MIN), BigInt::from(NUM::MAX)),
            _ => unreachable!("integer operand"),
        }),
        _ => unreachable!("numeric operand"),
    }
}

/// Values exercising storage, rounding and precision boundaries of a type:
/// zero, units, rounding ties, half and full range, and the machine word
/// boundaries crossed by native admission and divisor narrowing.
pub fn edges(data_type: &DataType) -> Vec<BigInt> {
    let (min, max) = range(data_type);
    let mut values = vec![
        BigInt::from(0),
        BigInt::from(1),
        BigInt::from(5),
        &max / 2_u8,
        max.clone(),
        min.clone(),
    ];
    if let DataType::Decimal(size) = data_type
        && size.precision() > 1
    {
        values.push(pow10(size.precision() as u32 - 1) * 5);
    }
    // i64 and i128 admission (and -2^63, -2^127 by negation), the low-word
    // truncation of 2^64 and 2^128, and the widest i256 product operands.
    for bits in [63, 127] {
        values.push((BigInt::from(1) << bits) - 1_u8);
    }
    for bits in [63, 64, 127, 128] {
        values.push(BigInt::from(1) << bits);
    }
    values.push((BigInt::from(1) << 128) - 1_u8);
    let mut values = values
        .into_iter()
        .flat_map(|value| [-value.clone(), value])
        .filter(|value| &min <= value && value <= &max)
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

/// Machine extremes of the storage, which no legal value reaches. Rows hidden
/// by validity may hold them.
pub fn hidden_payloads(data_type: &DataType) -> [BigInt; 2] {
    match data_type {
        DataType::Decimal(size) => with_decimal_mapped_type!(|T| match size.data_kind() {
            DecimalDataKind::T => [T::machine_min(), T::machine_max()].map(bigint),
        }),
        _ => {
            let (min, max) = range(data_type);
            [min, max]
        }
    }
}

trait MachineRange {
    fn machine_min() -> Self;
    fn machine_max() -> Self;
}

impl MachineRange for i64 {
    fn machine_min() -> Self {
        i64::MIN
    }
    fn machine_max() -> Self {
        i64::MAX
    }
}

impl MachineRange for i128 {
    fn machine_min() -> Self {
        i128::MIN
    }
    fn machine_max() -> Self {
        i128::MAX
    }
}

impl MachineRange for i256 {
    fn machine_min() -> Self {
        i256::from_words(i128::MIN, 0)
    }
    fn machine_max() -> Self {
        i256::from_words(i128::MAX, -1)
    }
}

pub fn bigint<T: DecimalExt>(value: T) -> BigInt {
    BigInt::from_signed_bytes_le(&value.as_decimal::<i256>().to_le_bytes())
}

fn decimal<T: DecimalExt>(value: &BigInt) -> T {
    let bytes = value.to_signed_bytes_le();
    let fill = if value < &BigInt::from(0) { 0xff } else { 0 };
    let mut words = [fill; 32];
    words[..bytes.len()].copy_from_slice(&bytes);
    i256::from_le_bytes(words).as_decimal::<T>()
}

/// A column of `data_type` holding `values` exactly, in canonical storage.
pub fn column(data_type: &DataType, values: &[BigInt]) -> Column {
    match data_type {
        DataType::Decimal(size) => with_decimal_mapped_type!(|T| match size.data_kind() {
            DecimalDataKind::T => {
                Column::Decimal(T::to_column(
                    values.iter().map(decimal::<T>).collect(),
                    *size,
                ))
            }
        }),
        DataType::Number(ty) => with_integer_mapped_type!(|NUM| match ty {
            NumberDataType::NUM => NumberType::<NUM>::from_data(
                values
                    .iter()
                    .map(|value| NUM::try_from(value.clone()).unwrap())
                    .collect(),
            ),
            _ => unreachable!("integer operand"),
        }),
        _ => unreachable!("numeric operand"),
    }
}

pub fn scalar_value(data_type: &DataType, value: &BigInt) -> Scalar {
    column(data_type, std::slice::from_ref(value))
        .index(0)
        .unwrap()
        .to_owned()
}

/// Decimal coefficient of every row of a decimal result.
pub fn rows(value: &Value<AnyType>, num_rows: usize) -> Vec<BigInt> {
    (0..num_rows)
        .map(|row| match value.index(row).unwrap() {
            ScalarRef::Decimal(value) => bigint(value.as_decimal::<i256>()),
            other => unreachable!("decimal result, got {other:?}"),
        })
        .collect()
}

pub fn context(
    func_ctx: &FunctionContext,
    num_rows: usize,
    validity: Option<Bitmap>,
) -> EvalContext<'_> {
    EvalContext {
        generics: &[],
        num_rows,
        func_ctx,
        validity,
        errors: None,
        suppress_error: false,
        strict_eval: true,
    }
}

pub fn error_rows(ctx: &EvalContext) -> Vec<bool> {
    let mut rows = vec![false; ctx.num_rows];
    if let Some((valid, _)) = &ctx.errors {
        for (row, valid) in valid.iter().enumerate() {
            rows[row] = !valid;
        }
    }
    rows
}

pub fn bind_function(
    registry: &FunctionRegistry,
    name: &str,
    params: Vec<Scalar>,
    args: &[DataType],
) -> Arc<Function> {
    let raw = RawExpr::FunctionCall {
        span: None,
        name: name.to_owned(),
        params,
        args: args
            .iter()
            .enumerate()
            .map(|(id, data_type)| RawExpr::ColumnRef {
                span: None,
                id,
                data_type: data_type.clone(),
                display_name: format!("c{id}"),
            })
            .collect(),
    };
    match type_check::check(&raw, registry).unwrap() {
        Expr::FunctionCall(call) => {
            assert!(
                call.args
                    .iter()
                    .all(|arg| matches!(arg, Expr::ColumnRef(_))),
                "{name}{args:?} must bind without casts"
            );
            call.function
        }
        other => panic!("{name}{args:?} bound to {other}"),
    }
}

/// Per-row expectation of a fallible function: `None` is an error.
pub type Expected = Option<BigInt>;

/// Evaluates `function` and checks every visible row against `expected`;
/// rows hidden by `visible` must neither error nor be inspected.
pub fn check_rows(
    function: &Function,
    args: &[Value<AnyType>],
    visible: Option<&[bool]>,
    expected: &[Expected],
    describe: impl Fn(usize) -> String,
) {
    let num_rows = expected.len();
    let func_ctx = FunctionContext::default();
    let validity = visible.map(|visible| visible.iter().copied().collect::<Bitmap>());
    let mut ctx = context(&func_ctx, num_rows, validity);
    let databend_common_expression::FunctionEval::Scalar { eval, .. } = &function.eval else {
        unreachable!("scalar function")
    };
    let value = eval.eval(args, &mut ctx);
    let errors = error_rows(&ctx);
    let actual = rows(&value, num_rows);
    for row in 0..num_rows {
        if !visible.is_none_or(|visible| visible[row]) {
            assert!(
                !errors[row],
                "hidden row reported an error: {}",
                describe(row)
            );
            continue;
        }
        match &expected[row] {
            None => assert!(errors[row], "expected an error: {}", describe(row)),
            Some(value) => {
                assert!(!errors[row], "unexpected error: {}", describe(row));
                assert_eq!(&actual[row], value, "{}", describe(row));
            }
        }
    }
}
