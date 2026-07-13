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

use databend_common_column::bitmap::Bitmap;

use super::ConstantFolder;
use crate::Column;
use crate::ColumnBuilder;
use crate::ColumnIndex;
use crate::EvalContext;
use crate::Expr;
use crate::Value;
use crate::function::ScalarFunction;
use crate::property::Domain;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;
use crate::types::string::StringDomain;
use crate::types::*;

impl<'a, Index: ColumnIndex> ConstantFolder<'a, Index> {
    pub(super) fn is_monotonic(&self, function_name: &str, args: &[Expr<Index>]) -> bool {
        self.fn_registry
            .properties
            .get(function_name)
            .is_some_and(|property| {
                if let [arg] = args {
                    property.monotonicity || property.monotonicity_by_type.contains(arg.data_type())
                } else {
                    false
                }
            })
    }

    pub(super) fn calculate_monotonicity_domain(
        &self,
        return_type: &DataType,
        input_domain: &Domain,
        generics: &[DataType],
        eval: &dyn ScalarFunction,
    ) -> Option<Domain> {
        let input = input_domain.boundary_column()?;
        let mut ctx = EvalContext {
            generics,
            num_rows: 2,
            validity: None,
            errors: None,
            func_ctx: self.func_ctx,
            suppress_error: false,
            strict_eval: true,
        };
        let Value::Column(col) = eval.eval(&[Value::Column(input)], &mut ctx) else {
            return None;
        };

        // if error happens, domain maybe incorrect
        // min, max: String("2024-09-02 00:00") String("2024-09-02 00:0�")
        // to_date(s) > to_date('2024-01-1')
        let domain = if ctx.has_error(0) || ctx.has_error(1) {
            // Preserve the successful boundary and widen only the failed side.
            // For example, a malformed minimum string must not discard a valid
            // maximum timestamp that can still prove an upper bound.
            // This assumes all currently registered monotonic functions are
            // non-decreasing. Supporting a decreasing function here requires
            // recording its direction in `FunctionProperty` and reversing the
            // fallback boundary.
            let full_domain = Domain::full(return_type);
            let Some(fallback) = full_domain.boundary_column() else {
                return Some(full_domain);
            };
            let mut builder = ColumnBuilder::with_capacity(return_type, 2);
            for (index, (value, fallback)) in col.iter().zip(fallback.iter()).enumerate() {
                if ctx.has_error(index) {
                    builder.push(fallback);
                } else {
                    builder.push(value);
                }
            }
            builder.build().domain()
        } else {
            col.domain()
        };

        if !return_type.is_nullable_or_null() {
            return Some(domain);
        }

        Some(match domain {
            Domain::Nullable(mut domain) => {
                domain.has_null = true;
                Domain::Nullable(domain)
            }
            domain => Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(domain)),
            }),
        })
    }
}

impl Domain {
    /// Materialize the finite, non-NULL endpoints used for monotonicity probing.
    fn boundary_column(&self) -> Option<Column> {
        Some(match self {
            Domain::Number(domain) => crate::with_number_type!(|NUM| match domain {
                NumberDomain::NUM(SimpleDomain { min, max }) =>
                    Column::Number(NumberColumn::NUM(Buffer::from(vec![*min, *max])),),
            }),
            Domain::Decimal(domain) => crate::with_decimal_type!(|DECIMAL| match domain {
                DecimalDomain::DECIMAL(SimpleDomain { min, max }, size) => Column::Decimal(
                    DecimalColumn::DECIMAL(Buffer::from(vec![*min, *max]), *size),
                ),
            }),
            Domain::Boolean(BooleanDomain {
                has_false,
                has_true,
            }) => match (*has_false, *has_true) {
                (true, true) => Column::Boolean(Bitmap::from([false, true])),
                (true, false) => Column::Boolean(Bitmap::from([false, false])),
                (false, true) => Column::Boolean(Bitmap::from([true, true])),
                (false, false) => return None,
            },
            Domain::String(StringDomain {
                min,
                max: Some(max),
            }) => Column::String(StringColumn::from_slice([min.as_str(), max.as_str()])),
            Domain::Timestamp(SimpleDomain { min, max }) => {
                Column::Timestamp(Buffer::from(vec![*min, *max]))
            }
            Domain::TimestampTz(SimpleDomain { min, max }) => {
                Column::TimestampTz(Buffer::from(vec![*min, *max]))
            }
            Domain::Date(SimpleDomain { min, max }) => Column::Date(Buffer::from(vec![*min, *max])),
            Domain::Interval(SimpleDomain { min, max }) => {
                Column::Interval(Buffer::from(vec![*min, *max]))
            }
            Domain::Nullable(NullableDomain {
                value: Some(domain),
                ..
            }) => NullableColumn::new_column(domain.boundary_column()?, Bitmap::new_trued(2)),
            _ => return None,
        })
    }
}
