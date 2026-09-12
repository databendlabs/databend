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
    /// Check if AND expressions contain mutually exclusive range conditions
    /// Returns Some(true) if the expressions are mutually exclusive (should return false)
    /// Returns Some(false) if they are not mutually exclusive
    /// Returns None if analysis is inconclusive
    pub(super) fn check_mutually_exclusive_ranges(
        &self,
        args: &[FoldResult<'_, Index>],
    ) -> Option<bool> {
        // Track constraints for each column
        let mut column_constraints: HashMap<Index, Vec<RangeConstraint<Index>>> = HashMap::new();

        // Extract constraints from each expression
        for arg in args {
            if let Some(constraint) = RangeConstraint::try_from_expr(arg.as_ref()) {
                column_constraints
                    .entry(constraint.column_id.clone())
                    .or_default()
                    .push(constraint);
            }
        }

        // Check for mutually exclusive constraints on each column
        for (_column_id, constraints) in column_constraints {
            if constraints.len() < 2 {
                continue;
            }

            // Check all pairs of constraints for mutual exclusion
            for i in 0..constraints.len() {
                for j in (i + 1)..constraints.len() {
                    if self.are_constraints_mutually_exclusive(&constraints[i], &constraints[j]) {
                        return Some(true); // Found mutually exclusive constraints
                    }
                }
            }

            if self.are_combined_constraints_mutually_exclusive(&constraints) {
                return Some(true);
            }
        }

        None // No conclusive mutual exclusion found
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

    fn are_combined_constraints_mutually_exclusive(
        &self,
        constraints: &[RangeConstraint<Index>],
    ) -> bool {
        let mut lower = None;
        let mut upper = None;
        let mut not_eq_constants = Vec::new();

        for constraint in constraints {
            match constraint.operator.as_str() {
                "gt" => tighten_lower_bound(&mut lower, &constraint.constant, false),
                "gte" => tighten_lower_bound(&mut lower, &constraint.constant, true),
                "lt" => tighten_upper_bound(&mut upper, &constraint.constant, false),
                "lte" => tighten_upper_bound(&mut upper, &constraint.constant, true),
                "noteq" => not_eq_constants.push(&constraint.constant),
                _ => {}
            }
        }

        let (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) = (&lower, &upper)
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

        not_eq_constants.contains(&lower)
    }
}

fn tighten_lower_bound(bound: &mut Option<(Scalar, bool)>, constant: &Scalar, inclusive: bool) {
    let should_update = bound.as_ref().is_none_or(|(current, current_inclusive)| {
        constant > current || (constant == current && !inclusive && *current_inclusive)
    });
    if should_update {
        *bound = Some((constant.clone(), inclusive));
    }
}

fn tighten_upper_bound(bound: &mut Option<(Scalar, bool)>, constant: &Scalar, inclusive: bool) {
    let should_update = bound.as_ref().is_none_or(|(current, current_inclusive)| {
        constant < current || (constant == current && !inclusive && *current_inclusive)
    });
    if should_update {
        *bound = Some((constant.clone(), inclusive));
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
    pub operator: String, // "gt", "gte", "lt", "lte", "eq"
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
