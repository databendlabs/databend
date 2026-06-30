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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::F64;
use databend_common_functions::SPATIAL_INDEX_FUNCTIONS;
use unicase::Ascii;

use crate::ColumnSet;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::Side;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::SpatialJoinCandidate;

/// Finalize derived spatial join annotations after logical rewrites have settled.
pub struct FinalizeSpatialJoinOptimizer {
    ctx: Arc<OptimizerContext>,
}

impl FinalizeSpatialJoinOptimizer {
    pub fn new(ctx: Arc<OptimizerContext>) -> Self {
        Self { ctx }
    }

    pub fn optimize_sync(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        if !self
            .ctx
            .get_table_ctx()
            .get_settings()
            .get_enable_spatial_join()?
        {
            return Ok(s_expr.clone());
        }

        Self::finalize_spatial_join(s_expr)
    }

    #[recursive::recursive]
    fn finalize_spatial_join(s_expr: &SExpr) -> Result<SExpr> {
        let mut changed = false;
        let mut children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children() {
            let new_child = Self::finalize_spatial_join(child)?;
            changed |= !new_child.eq(child);
            children.push(Arc::new(new_child));
        }

        let mut result = if changed {
            s_expr.replace_children(children)
        } else {
            s_expr.clone()
        };

        if let RelOperator::Join(join) = result.plan() {
            let left_prop = RelExpr::with_s_expr(result.left_child()).derive_relational_prop()?;
            let right_prop = RelExpr::with_s_expr(result.right_child()).derive_relational_prop()?;
            let spatial_join =
                spatial_join_gate(join, &left_prop.output_columns, &right_prop.output_columns)
                    .map(Box::new);

            if join.spatial_join != spatial_join {
                let mut join = join.clone();
                join.spatial_join = spatial_join;
                result = result.replace_plan(Arc::new(RelOperator::Join(join)));
            }
        }

        Ok(result)
    }
}

#[async_trait::async_trait]
impl Optimizer for FinalizeSpatialJoinOptimizer {
    fn name(&self) -> String {
        "FinalizeSpatialJoinOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}

fn spatial_join_gate(
    join: &Join,
    left_output_columns: &ColumnSet,
    right_output_columns: &ColumnSet,
) -> Option<SpatialJoinCandidate> {
    // SpatialJoin is a local non-equi inner join path. Joins that require hash
    // keys, cache-scan state, or single-to-inner runtime handling stay on the
    // regular join path.
    if join.join_type != JoinType::Inner
        || !join.equi_conditions.is_empty()
        || join.non_equi_conditions.len() != 1
        || join.build_side_cache_info.is_some()
        || join.single_to_inner.is_some()
    {
        return None;
    }

    let predicate = &join.non_equi_conditions[0];
    let ScalarExpr::FunctionCall(function) = predicate else {
        return None;
    };

    let func_name = function.func_name.as_ref();
    let uni_case_func_name = Ascii::new(func_name);
    if !SPATIAL_INDEX_FUNCTIONS.contains(&(uni_case_func_name, function.arguments.len())) {
        return None;
    }

    // The spatial join evaluates predicate arguments for index probing separately
    // from the final predicate filter. Non-deterministic arguments could diverge
    // between those evaluations and drop valid row pairs.
    if !function.arguments.iter().all(ScalarExpr::is_deterministic) {
        return None;
    }

    let distance = match function.arguments.get(2) {
        Some(ScalarExpr::ConstantExpr(constant))
        | Some(ScalarExpr::TypedConstantExpr(constant, _)) => {
            Some(constant.value.to_distance_threshold().map(F64::from)?)
        }
        Some(_) => return None,
        None => None,
    };
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

    // The first two spatial function arguments are the geometry expressions used
    // by the R-tree path. Normalize them to the logical left/right input order;
    // the original predicate is still kept for exact filtering.
    let (left_geometry, right_geometry) = match (first_side, second_side) {
        (Some(Side::Left), Some(Side::Right)) => {
            (function.arguments[0].clone(), function.arguments[1].clone())
        }
        (Some(Side::Right), Some(Side::Left)) => {
            (function.arguments[1].clone(), function.arguments[0].clone())
        }
        _ => return None,
    };

    Some(SpatialJoinCandidate {
        predicate: predicate.clone(),
        left_geometry,
        right_geometry,
        distance,
    })
}

fn spatial_argument_side(
    argument: &ScalarExpr,
    left_output_columns: &ColumnSet,
    right_output_columns: &ColumnSet,
) -> Option<Side> {
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
        (true, false) => Some(Side::Left),
        (false, true) => Some(Side::Right),
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
    fn test_spatial_join_gate_rejects_residual_predicate() {
        let spatial_predicate = function_call("st_intersects", vec![
            column(0, DataType::Geometry),
            column(2, DataType::Geometry),
        ]);
        let residual_predicate = function_call("gt", vec![
            column(1, DataType::Number(NumberDataType::Int32)),
            column(3, DataType::Number(NumberDataType::Int32)),
        ]);
        let join = spatial_join(vec![spatial_predicate, residual_predicate]);

        assert_eq!(
            spatial_join_gate(&join, &column_set(&[0, 1]), &column_set(&[2, 3])),
            None
        );
    }

    #[test]
    fn test_spatial_join_gate_normalizes_reversed_arguments() {
        let join = spatial_join(vec![function_call("ST_Contains", vec![
            column(2, DataType::Geometry),
            column(0, DataType::Geometry),
        ])]);

        let gate = spatial_join_gate(&join, &column_set(&[0]), &column_set(&[2]));
        match gate {
            Some(candidate) => {
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
        for (function_name, arg_count) in SPATIAL_INDEX_FUNCTIONS {
            let function_name = function_name.into_inner();
            let mut arguments = vec![column(0, DataType::Geometry), column(1, DataType::Geometry)];
            if arg_count == 3 {
                arguments.push(constant_i32(10));
            }

            let join = spatial_join(vec![function_call(function_name, arguments)]);

            let candidate = spatial_join_gate(&join, &column_set(&[0]), &column_set(&[1]))
                .unwrap_or_else(|| panic!("{function_name} should be accepted"));
            assert_eq!(candidate.distance.is_some(), arg_count == 3);
        }
    }

    #[test]
    fn test_spatial_join_gate_accepts_dwithin_with_constant_distance() {
        let predicate = function_call("st_dwithin", vec![
            column(0, DataType::Geometry),
            column(1, DataType::Geometry),
            constant_i32(10),
        ]);
        let join = spatial_join(vec![predicate.clone()]);

        let candidate = spatial_join_gate(&join, &column_set(&[0]), &column_set(&[1]))
            .expect("DWithin should be accepted");
        assert_eq!(candidate.predicate, predicate);
        assert_eq!(candidate.left_geometry, column(0, DataType::Geometry));
        assert_eq!(candidate.right_geometry, column(1, DataType::Geometry));
        assert_eq!(candidate.distance, Some(F64::from(10.0)));
    }

    #[test]
    fn test_spatial_join_gate_rejects_dwithin_with_non_constant_distance() {
        let join = spatial_join(vec![function_call("st_dwithin", vec![
            column(0, DataType::Geometry),
            column(1, DataType::Geometry),
            column(2, DataType::Number(NumberDataType::Int32)),
        ])]);

        assert_eq!(
            spatial_join_gate(&join, &column_set(&[0]), &column_set(&[1, 2])),
            None
        );
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
            None
        );

        let same_side_join = spatial_join(vec![function_call("st_intersects", vec![
            column(0, DataType::Geometry),
            column(1, DataType::Geometry),
        ])]);
        assert_eq!(
            spatial_join_gate(&same_side_join, &column_set(&[0, 1]), &column_set(&[2])),
            None
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
            None
        );
    }
}
