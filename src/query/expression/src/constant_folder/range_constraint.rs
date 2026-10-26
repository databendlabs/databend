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

use std::cmp::Ordering;
use std::collections::HashMap;

use super::ConstantFolder;
use super::FoldResult;
use crate::ColumnIndex;
use crate::Scalar;
use crate::expression::Cast;
use crate::expression::Constant;
use crate::expression::Expr;
use crate::expression::FunctionCall;
use crate::types::DataType;

impl<'a, Index: ColumnIndex> ConstantFolder<'a, Index> {
    /// Check if AND expressions contain mutually exclusive range conditions.
    /// Returns Some(true) if no row can satisfy the comparisons, otherwise None.
    /// An inconclusive result does not prove that the expressions are satisfiable.
    pub(super) fn check_mutually_exclusive_ranges(
        &self,
        args: &[FoldResult<'_, Index>],
    ) -> Option<bool> {
        let mut column_constraints: HashMap<(Index, DataType), ConstraintState> = HashMap::new();

        for arg in args {
            let Some(constraint) = RangeConstraint::try_from_expr(arg.as_ref()) else {
                continue;
            };
            // Scalar ordering does not model mixed-type SQL comparisons. NULL
            // comparisons are handled by the caller's boolean domain analysis.
            if constraint.constant.as_ref().infer_data_type()
                != constraint.data_type.remove_nullable()
            {
                continue;
            }

            let state = column_constraints
                .entry((constraint.column_id, constraint.data_type))
                .or_default();
            if state.add(&constraint.operator, constraint.constant) {
                return Some(true);
            }
        }

        column_constraints
            .values()
            .any(ConstraintState::is_mutually_exclusive)
            .then_some(true)
    }

    /// Check if two range constraints are mutually exclusive
    pub fn are_constraints_mutually_exclusive(
        &self,
        c1: &RangeConstraint<Index>,
        c2: &RangeConstraint<Index>,
    ) -> bool {
        // Only check constraints on the same column with the same data type
        if c1.column_id != c2.column_id || c1.data_type != c2.data_type {
            return false;
        }

        // Check for patterns like: x > a AND x < b where a >= b
        // or x >= a AND x < b where a >= b
        // or x > a AND x <= b where a >= b
        // or x >= a AND x <= b where a > b
        match (c1.operator.as_str(), c2.operator.as_str()) {
            ("gt", "lt") => c1.constant >= c2.constant,
            ("lt", "gt") => c2.constant >= c1.constant,
            ("gt", "lte") => c1.constant >= c2.constant,
            ("lte", "gt") => c2.constant >= c1.constant,
            ("gte", "lt") => c1.constant >= c2.constant,
            ("lt", "gte") => c2.constant >= c1.constant,
            ("gte", "lte") => c1.constant > c2.constant,
            ("lte", "gte") => c2.constant > c1.constant,
            ("eq", "gt") => {
                // x = a AND x > b where a <= b
                c1.constant <= c2.constant
            }
            ("gt", "eq") => {
                // x > a AND x = b where b <= a
                c2.constant <= c1.constant
            }
            ("eq", "gte") => {
                // x = a AND x >= b where a < b
                c1.constant < c2.constant
            }
            ("gte", "eq") => {
                // x >= a AND x = b where b < a
                c2.constant < c1.constant
            }
            ("eq", "lt") => {
                // x = a AND x < b where a >= b
                c1.constant >= c2.constant
            }
            ("lt", "eq") => {
                // x < a AND x = b where b >= a
                c2.constant >= c1.constant
            }
            ("eq", "lte") => {
                // x = a AND x <= b where a > b
                c1.constant > c2.constant
            }
            ("lte", "eq") => {
                // x <= a AND x = b where b > a
                c2.constant > c1.constant
            }
            ("eq", "eq") => {
                // x = a AND x = b where a != b
                c1.constant != c2.constant
            }
            ("eq", "noteq") => {
                // x = a AND x != b where a == b
                c1.constant == c2.constant
            }
            ("noteq", "eq") => {
                // x != a AND x = b where a == b
                c1.constant == c2.constant
            }
            _ => false,
        }
    }
}

/// Summary of compatible comparisons on one column. Accumulating bounds and
/// equality takes constant comparisons per constraint; exclusions are scanned
/// only once, after the final singleton candidate is known.
#[derive(Default)]
struct ConstraintState {
    lower: Option<(Scalar, bool)>,
    upper: Option<(Scalar, bool)>,
    equal: Option<Scalar>,
    not_eq_constants: Vec<Scalar>,
}

impl ConstraintState {
    /// Returns true if distinct equality constraints already prove a conflict.
    fn add(&mut self, operator: &str, constant: Scalar) -> bool {
        match operator {
            "gt" => tighten_lower_bound(&mut self.lower, constant, false),
            "gte" => tighten_lower_bound(&mut self.lower, constant, true),
            "lt" => tighten_upper_bound(&mut self.upper, constant, false),
            "lte" => tighten_upper_bound(&mut self.upper, constant, true),
            "eq" => {
                if let Some(equal) = &self.equal {
                    return matches!(
                        equal.partial_cmp(&constant),
                        Some(Ordering::Less | Ordering::Greater)
                    );
                }
                self.equal = Some(constant);
            }
            "noteq" => self.not_eq_constants.push(constant),
            _ => {}
        }
        false
    }

    fn is_mutually_exclusive(&self) -> bool {
        if let Some(equal) = &self.equal {
            let below_lower = self
                .lower
                .as_ref()
                .is_some_and(|(lower, inclusive)| equal < lower || (equal == lower && !inclusive));
            let above_upper = self
                .upper
                .as_ref()
                .is_some_and(|(upper, inclusive)| equal > upper || (equal == upper && !inclusive));
            return below_lower || above_upper || self.not_eq_constants.contains(equal);
        }

        let (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) =
            (&self.lower, &self.upper)
        else {
            return false;
        };

        if lower > upper {
            return true;
        }
        if lower != upper {
            return false;
        }
        if !lower_inclusive || !upper_inclusive {
            return true;
        }

        self.not_eq_constants.contains(lower)
    }
}

fn tighten_lower_bound(bound: &mut Option<(Scalar, bool)>, constant: Scalar, inclusive: bool) {
    let should_update = bound.as_ref().is_none_or(|(current, current_inclusive)| {
        &constant > current || (&constant == current && !inclusive && *current_inclusive)
    });
    if should_update {
        *bound = Some((constant, inclusive));
    }
}

fn tighten_upper_bound(bound: &mut Option<(Scalar, bool)>, constant: Scalar, inclusive: bool) {
    let should_update = bound.as_ref().is_none_or(|(current, current_inclusive)| {
        &constant < current || (&constant == current && !inclusive && *current_inclusive)
    });
    if should_update {
        *bound = Some((constant, inclusive));
    }
}

fn constant_behind_nullable_cast<Index: ColumnIndex>(expr: &Expr<Index>) -> Option<&Constant> {
    if let Expr::Constant(constant) = expr {
        return Some(constant);
    }

    let Expr::Cast(Cast {
        is_try: false,
        expr,
        dest_type,
        ..
    }) = expr
    else {
        return None;
    };

    let Expr::Constant(constant) = expr.as_ref() else {
        return None;
    };

    (dest_type.is_nullable()
        && !constant.data_type.is_nullable()
        && dest_type.remove_nullable() == constant.data_type)
        .then_some(constant)
}

/// Represents a range constraint extracted from a comparison expression
#[derive(Debug, Clone)]
pub struct RangeConstraint<Index> {
    pub column_id: Index,
    pub data_type: DataType,
    pub operator: String, // "gt", "gte", "lt", "lte", "eq", "noteq"
    pub constant: Scalar,
    pub is_flipped: bool, // true if original was constant op column
}

impl<Index: ColumnIndex> RangeConstraint<Index> {
    /// Extracts a normalized column-to-constant comparison. Comparisons with
    /// the constant on the left are flipped so the column is always the lhs.
    pub fn try_from_expr(expr: &Expr<Index>) -> Option<Self> {
        let Expr::FunctionCall(call) = expr else {
            return None;
        };
        Self::try_from_function_call(call)
    }

    pub fn try_from_function_call(call: &FunctionCall<Index>) -> Option<Self> {
        let FunctionCall { function, args, .. } = call;
        if args.len() != 2 {
            return None;
        }

        let op = function.signature.name.as_str();
        if !matches!(op, "gt" | "gte" | "lt" | "lte" | "eq" | "noteq") {
            return None;
        }

        if let (Some(column_ref), Some(constant)) = (
            args[0].as_column_ref(),
            constant_behind_nullable_cast(&args[1]),
        ) {
            return Some(Self {
                column_id: column_ref.id.clone(),
                data_type: column_ref.data_type.clone(),
                operator: op.to_string(),
                constant: constant.scalar.clone(),
                is_flipped: false,
            });
        }

        let (Some(constant), Some(column_ref)) = (
            constant_behind_nullable_cast(&args[0]),
            args[1].as_column_ref(),
        ) else {
            return None;
        };
        let operator = match op {
            "gt" => "lt",
            "gte" => "lte",
            "lt" => "gt",
            "lte" => "gte",
            "eq" => "eq",
            "noteq" => "noteq",
            _ => unreachable!(),
        };
        Some(Self {
            column_id: column_ref.id.clone(),
            data_type: column_ref.data_type.clone(),
            operator: operator.to_string(),
            constant: constant.scalar.clone(),
            is_flipped: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::HashMap;

    use super::ConstantFolder;
    use super::ConstraintState;
    use super::FoldResult;
    use crate::FunctionContext;
    use crate::FunctionDomain;
    use crate::FunctionRegistry;
    use crate::Scalar;
    use crate::expression::ColumnRef;
    use crate::expression::Constant;
    use crate::expression::Expr;
    use crate::type_check::check_function;
    use crate::types::BooleanType;
    use crate::types::DataType;
    use crate::types::F64;
    use crate::types::NumberDataType;
    use crate::types::NumberScalar;
    use crate::types::UInt64Type;

    #[test]
    fn test_constraint_state_nan_singleton_exclusion() {
        let nan = Scalar::Number(NumberScalar::Float64(F64::from(f64::NAN)));
        for operators in [["noteq", "gte", "lte"], ["gte", "lte", "noteq"]] {
            let mut state = ConstraintState::default();
            for operator in operators {
                assert!(!state.add(operator, nan.clone()));
            }
            assert!(state.is_mutually_exclusive());
        }
    }

    #[test]
    fn test_range_check_keeps_column_types_separate() {
        let mut registry = FunctionRegistry::empty();
        for operator in ["gte", "lte", "noteq"] {
            registry
                .register_passthrough_nullable_2_arg::<UInt64Type, UInt64Type, BooleanType, _, _>(
                    operator,
                    |_, _, _| FunctionDomain::Full,
                    |_, _, _| crate::Value::Scalar(false),
                );
        }
        let registry = &registry;
        let data_type = DataType::Number(NumberDataType::UInt64);
        let constant = Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Number(NumberScalar::UInt64(5)),
            data_type: data_type.clone(),
        });
        let args = [
            ("gte", data_type.clone()),
            ("lte", data_type.clone().wrap_nullable()),
            ("noteq", data_type),
        ]
        .into_iter()
        .map(|(operator, data_type)| {
            let column = Expr::ColumnRef(ColumnRef {
                span: None,
                id: 0_usize,
                data_type,
                display_name: "x".to_string(),
            });
            let expr =
                check_function(None, operator, &[], &[column, constant.clone()], registry).unwrap();
            FoldResult::Unchanged(Cow::Owned(expr))
        })
        .collect::<Vec<_>>();
        let domains = HashMap::new();
        let context = FunctionContext::default();
        let folder = ConstantFolder::new_for_test(&domains, &context, registry);
        assert_eq!(folder.check_mutually_exclusive_ranges(&args), None);
    }

    #[test]
    fn test_constraint_state_all_comparison_triples() {
        let operators = ["gt", "gte", "lt", "lte", "eq", "noteq"];
        let constraints = operators
            .iter()
            .flat_map(|&operator| [1_u64, 3, 5].map(|value| (operator, value)))
            .collect::<Vec<_>>();

        // The candidates cover the endpoints, each gap and both outer ranges.
        // Enumerating ordered triples also checks every argument permutation.
        for &first in &constraints {
            for &second in &constraints {
                for &third in &constraints {
                    let triple = [first, second, third];
                    let mut state = ConstraintState::default();
                    let conflict = triple.iter().any(|&(operator, value)| {
                        state.add(operator, Scalar::Number(NumberScalar::UInt64(value)))
                    }) || state.is_mutually_exclusive();
                    let has_solution = (0..=6).any(|candidate| {
                        triple.iter().all(|&(operator, value)| match operator {
                            "gt" => candidate > value,
                            "gte" => candidate >= value,
                            "lt" => candidate < value,
                            "lte" => candidate <= value,
                            "eq" => candidate == value,
                            "noteq" => candidate != value,
                            _ => unreachable!(),
                        })
                    });
                    assert_eq!(conflict, !has_solution, "{triple:?}");
                }
            }
        }
    }
}
