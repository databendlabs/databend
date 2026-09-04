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

use super::ConstantFolder;
use crate::ColumnBuilder;
use crate::ColumnIndex;
use crate::EvalContext;
use crate::Scalar;
use crate::Value;
use crate::function::ScalarFunction;
use crate::property::Domain;
use crate::types::DataType;
use crate::types::nullable::NullableDomain;

impl<'a, Index: ColumnIndex> ConstantFolder<'a, Index> {
    pub(super) fn is_monotonic(
        &self,
        function_name: &str,
        argument_count: usize,
        argument_type: Option<&DataType>,
        argument_domains: &[Domain],
    ) -> bool {
        let Some(property) = self.fn_registry.properties.get(function_name) else {
            return false;
        };
        let is_monotonicity = argument_type.is_some_and(|argument_type| {
            argument_count == 1
                && (property.monotonicity || property.monotonicity_by_type.contains(argument_type))
        });
        let monotonicity_check = property.monotonicity_check.filter(|_| argument_count == 1);

        // Range-sensitive checks complement the static flags: they may prove monotonicity for
        // this specific argument range and context only.
        is_monotonicity
            || monotonicity_check
                .is_some_and(|check| check(self.func_ctx, argument_domains) == Some(0))
    }

    pub(super) fn calculate_monotonicity_domain(
        &self,
        return_type: &DataType,
        argument_type: &DataType,
        domain: &Domain,
        generics: &[DataType],
        eval: &dyn ScalarFunction,
    ) -> Option<Domain> {
        let (value_domain, has_null) = match domain {
            Domain::Nullable(NullableDomain { has_null, value }) => (value.as_deref(), *has_null),
            domain => (Some(domain), false),
        };

        let mut boundaries = Vec::with_capacity(3);
        if let Some(value_domain) = value_domain {
            let (min, max) = value_domain.to_minmax();
            if min.is_null() || max.is_null() {
                return None;
            }
            boundaries.extend([min, max]);
        }
        if has_null {
            boundaries.push(Scalar::Null);
        }
        if boundaries.is_empty() {
            return None;
        }

        let mut ctx = EvalContext {
            generics,
            num_rows: boundaries.len(),
            validity: None,
            errors: None,
            func_ctx: self.func_ctx,
            suppress_error: false,
            strict_eval: true,
        };
        let mut builder = ColumnBuilder::with_capacity(argument_type, boundaries.len());
        for boundary in &boundaries {
            builder.push(boundary.as_ref());
        }

        let input = Value::Column(builder.build());
        let result = eval.eval(&[input], &mut ctx);

        if result.is_scalar() {
            return None;
        }

        // if error happens, domain maybe incorrect
        // min, max: String("2024-09-02 00:00") String("2024-09-02 00:0�")
        // to_date(s) > to_date('2024-01-1')
        let col = result.as_column().unwrap();
        let domain = if boundaries
            .iter()
            .enumerate()
            .any(|(index, _)| ctx.has_error(index))
        {
            // NULL is not an ordered boundary. If evaluating it fails, the function's domain is
            // not known.
            if has_null && ctx.has_error(boundaries.len() - 1) {
                return None;
            }

            let full_domain = Domain::full(return_type);
            let full_value_domain = match &full_domain {
                Domain::Nullable(NullableDomain { value, .. }) => value.as_deref()?,
                domain => domain,
            };
            let (full_min, full_max) = full_value_domain.to_minmax();
            if full_min.is_null() || full_max.is_null() {
                return None;
            }

            let mut builder = ColumnBuilder::with_capacity(return_type, boundaries.len());

            for (index, value) in col.iter().enumerate() {
                if ctx.has_error(index) {
                    let fallback = if index == 0 { &full_min } else { &full_max };
                    builder.push(fallback.as_ref());
                } else {
                    builder.push(value);
                }
            }
            builder.build().domain()
        } else {
            result.as_column().unwrap().domain()
        };
        Some(domain)
    }
}
