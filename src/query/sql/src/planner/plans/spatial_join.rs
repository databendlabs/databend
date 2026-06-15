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

use crate::ColumnSet;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SpatialJoinCandidate {
    pub predicate: ScalarExpr,
    pub left_geometry: ScalarExpr,
    pub right_geometry: ScalarExpr,
    pub residual_predicates: Vec<ScalarExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SpatialJoinRejectReason {
    UnsupportedJoinType(JoinType),
    NoSpatialPredicate,
    MultipleSpatialPredicates,
    InvalidSpatialPredicateArguments,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpatialJoinArgumentSide {
    Left,
    Right,
}

pub type SpatialJoinGateResult = Result<SpatialJoinCandidate, SpatialJoinRejectReason>;

pub fn spatial_join_gate(
    join: &Join,
    left_output_columns: &ColumnSet,
    right_output_columns: &ColumnSet,
) -> SpatialJoinGateResult {
    if join.join_type != JoinType::Inner {
        return Err(SpatialJoinRejectReason::UnsupportedJoinType(join.join_type));
    }

    let mut spatial_predicate = None;
    let mut residual_predicates = Vec::new();

    for predicate in &join.non_equi_conditions {
        if is_spatial_predicate(predicate) {
            if spatial_predicate.is_some() {
                return Err(SpatialJoinRejectReason::MultipleSpatialPredicates);
            }

            match extract_spatial_join_candidate(
                predicate,
                left_output_columns,
                right_output_columns,
            ) {
                Ok(predicate) => spatial_predicate = Some(predicate),
                Err(reason) => return Err(reason),
            }
        } else {
            residual_predicates.push(predicate.clone());
        }
    }

    match spatial_predicate {
        Some(mut candidate) => {
            candidate.residual_predicates = residual_predicates;
            Ok(candidate)
        }
        None => Err(SpatialJoinRejectReason::NoSpatialPredicate),
    }
}

fn extract_spatial_join_candidate(
    predicate: &ScalarExpr,
    left_output_columns: &ColumnSet,
    right_output_columns: &ColumnSet,
) -> SpatialJoinGateResult {
    let ScalarExpr::FunctionCall(function) = predicate else {
        return Err(SpatialJoinRejectReason::NoSpatialPredicate);
    };

    if function.arguments.len() != 2 {
        return Err(SpatialJoinRejectReason::InvalidSpatialPredicateArguments);
    }

    let first_side = spatial_argument_side(
        &function.arguments[0],
        left_output_columns,
        right_output_columns,
    );
    let second_side = spatial_argument_side(
        &function.arguments[1],
        left_output_columns,
        right_output_columns,
    );

    // The physical operator only uses the geometries to feed the bbox prefilter
    // and re-runs the original `predicate` for the exact test, so we just need to
    // know which geometry comes from which side; the predicate kind is irrelevant.
    let (left_geometry, right_geometry) = match (first_side, second_side) {
        (Some(SpatialJoinArgumentSide::Left), Some(SpatialJoinArgumentSide::Right)) => {
            (function.arguments[0].clone(), function.arguments[1].clone())
        }
        (Some(SpatialJoinArgumentSide::Right), Some(SpatialJoinArgumentSide::Left)) => {
            (function.arguments[1].clone(), function.arguments[0].clone())
        }
        _ => return Err(SpatialJoinRejectReason::InvalidSpatialPredicateArguments),
    };

    Ok(SpatialJoinCandidate {
        predicate: predicate.clone(),
        left_geometry,
        right_geometry,
        residual_predicates: Vec::new(),
    })
}

fn is_spatial_predicate(predicate: &ScalarExpr) -> bool {
    let ScalarExpr::FunctionCall(function) = predicate else {
        return false;
    };

    matches!(
        function.func_name.to_ascii_lowercase().as_str(),
        "st_intersects" | "st_within" | "st_contains" | "st_covers" | "st_coveredby"
    )
}

fn spatial_argument_side(
    argument: &ScalarExpr,
    left_output_columns: &ColumnSet,
    right_output_columns: &ColumnSet,
) -> Option<SpatialJoinArgumentSide> {
    let used_columns = argument.used_columns();
    if used_columns.is_empty() {
        return None;
    }

    let uses_only = |columns: &ColumnSet, other_columns: &ColumnSet| {
        used_columns.is_subset(columns) && used_columns.is_disjoint(other_columns)
    };

    match (
        uses_only(left_output_columns, right_output_columns),
        uses_only(right_output_columns, left_output_columns),
    ) {
        (true, false) => Some(SpatialJoinArgumentSide::Left),
        (false, true) => Some(SpatialJoinArgumentSide::Right),
        _ => None,
    }
}
#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Symbol;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;
    use crate::plans::ConstantExpr;
    use crate::plans::FunctionCall;

    fn column(index: usize, data_type: DataType) -> ScalarExpr {
        ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(data_type),
                Visibility::Visible,
            )
            .build(),
        })
    }

    fn function_call(func_name: &str, arguments: Vec<ScalarExpr>) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: func_name.to_string(),
            params: vec![],
            arguments,
        })
    }

    fn constant_i32(value: i32) -> ScalarExpr {
        ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::Number(NumberScalar::Int32(value)),
        })
    }

    fn column_set(indices: &[usize]) -> ColumnSet {
        indices.iter().copied().map(Symbol::new).collect()
    }

    fn spatial_join(non_equi_conditions: Vec<ScalarExpr>) -> Join {
        Join {
            non_equi_conditions,
            join_type: JoinType::Inner,
            ..Default::default()
        }
    }

    #[test]
    fn test_spatial_join_gate_accepts_spatial_predicate_with_residual() {
        let spatial_predicate = function_call("st_intersects", vec![
            column(0, DataType::Geometry),
            column(2, DataType::Geometry),
        ]);
        let residual_predicate = function_call("gt", vec![
            column(1, DataType::Number(NumberDataType::Int32)),
            column(3, DataType::Number(NumberDataType::Int32)),
        ]);
        let join = spatial_join(vec![spatial_predicate.clone(), residual_predicate.clone()]);

        let gate = spatial_join_gate(&join, &column_set(&[0, 1]), &column_set(&[2, 3]));
        match gate {
            Ok(candidate) => {
                assert_eq!(candidate.predicate, spatial_predicate);
                assert_eq!(candidate.left_geometry, column(0, DataType::Geometry));
                assert_eq!(candidate.right_geometry, column(2, DataType::Geometry));
                assert_eq!(candidate.residual_predicates, vec![residual_predicate]);
            }
            other => panic!("unexpected spatial join gate result: {other:?}"),
        }
    }

    #[test]
    fn test_spatial_join_gate_normalizes_reversed_arguments() {
        let join = spatial_join(vec![function_call("ST_Contains", vec![
            column(2, DataType::Geometry),
            column(0, DataType::Geometry),
        ])]);

        let gate = spatial_join_gate(&join, &column_set(&[0]), &column_set(&[2]));
        match gate {
            Ok(candidate) => {
                assert_eq!(candidate.left_geometry, column(0, DataType::Geometry));
                assert_eq!(candidate.right_geometry, column(2, DataType::Geometry));
            }
            other => panic!("unexpected spatial join gate result: {other:?}"),
        }

        let join = spatial_join(vec![function_call("st_covers", vec![
            column(2, DataType::Geometry),
            column(0, DataType::Geometry),
        ])]);

        let candidate = spatial_join_gate(&join, &column_set(&[0]), &column_set(&[2]))
            .expect("reversed spatial predicate should be accepted");
        assert_eq!(candidate.left_geometry, column(0, DataType::Geometry));
        assert_eq!(candidate.right_geometry, column(2, DataType::Geometry));
    }

    #[test]
    fn test_spatial_join_gate_accepts_all_spatial_index_predicates() {
        for function_name in [
            "st_intersects",
            "st_within",
            "st_contains",
            "st_covers",
            "st_coveredby",
        ] {
            let join = spatial_join(vec![function_call(function_name, vec![
                column(0, DataType::Geometry),
                column(1, DataType::Geometry),
            ])]);

            spatial_join_gate(&join, &column_set(&[0]), &column_set(&[1]))
                .unwrap_or_else(|_| panic!("{function_name} should be accepted"));
        }
    }

    #[test]
    fn test_spatial_join_gate_treats_dwithin_as_residual() {
        let join = spatial_join(vec![function_call("st_dwithin", vec![
            column(0, DataType::Geometry),
            column(1, DataType::Geometry),
            constant_i32(10),
        ])]);

        assert_eq!(
            spatial_join_gate(&join, &column_set(&[0]), &column_set(&[1])),
            Err(SpatialJoinRejectReason::NoSpatialPredicate)
        );

        let join = spatial_join(vec![
            function_call("st_intersects", vec![
                column(0, DataType::Geometry),
                column(1, DataType::Geometry),
            ]),
            function_call("st_dwithin", vec![
                column(0, DataType::Geometry),
                column(1, DataType::Geometry),
                constant_i32(10),
            ]),
        ]);

        let candidate = spatial_join_gate(&join, &column_set(&[0]), &column_set(&[1]))
            .expect("spatial predicate with a DWithin residual should be accepted");
        assert_eq!(candidate.residual_predicates.len(), 1);
    }

    #[test]
    fn test_spatial_join_gate_rejects_unsupported_shape() {
        let mut left_join = spatial_join(vec![function_call("st_within", vec![
            column(0, DataType::Geometry),
            column(1, DataType::Geometry),
        ])]);
        left_join.join_type = JoinType::Left;
        assert_eq!(
            spatial_join_gate(&left_join, &column_set(&[0]), &column_set(&[1])),
            Err(SpatialJoinRejectReason::UnsupportedJoinType(JoinType::Left))
        );

        let same_side_join = spatial_join(vec![function_call("st_intersects", vec![
            column(0, DataType::Geometry),
            column(1, DataType::Geometry),
        ])]);
        assert_eq!(
            spatial_join_gate(&same_side_join, &column_set(&[0, 1]), &column_set(&[2])),
            Err(SpatialJoinRejectReason::InvalidSpatialPredicateArguments)
        );

        let multi_spatial_join = spatial_join(vec![
            function_call("st_intersects", vec![
                column(0, DataType::Geometry),
                column(2, DataType::Geometry),
            ]),
            function_call("st_contains", vec![
                column(1, DataType::Geometry),
                column(3, DataType::Geometry),
            ]),
        ]);
        assert_eq!(
            spatial_join_gate(
                &multi_spatial_join,
                &column_set(&[0, 1]),
                &column_set(&[2, 3])
            ),
            Err(SpatialJoinRejectReason::MultipleSpatialPredicates)
        );
    }
}
