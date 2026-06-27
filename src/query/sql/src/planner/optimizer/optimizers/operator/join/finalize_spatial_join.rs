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

use crate::optimizer::Optimizer;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::RelOperator;
use crate::plans::spatial_join_gate;

/// Finalize derived spatial join annotations after logical rewrites have settled.
pub struct FinalizeSpatialJoinOptimizer {}

impl FinalizeSpatialJoinOptimizer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn optimize_sync(&mut self, s_expr: &SExpr) -> Result<SExpr> {
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
                    .ok()
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

impl Default for FinalizeSpatialJoinOptimizer {
    fn default() -> Self {
        Self::new()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::types::DataType;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::ColumnSet;
    use crate::Symbol;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;
    use crate::plans::FunctionCall;
    use crate::plans::Join;
    use crate::plans::JoinType;
    use crate::plans::ScalarExpr;
    use crate::plans::Scan;

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

    fn scan(indices: &[usize]) -> Arc<SExpr> {
        let columns: ColumnSet = indices.iter().copied().map(Symbol::new).collect();
        Arc::new(SExpr::create_leaf(Arc::new(RelOperator::Scan(Scan {
            columns,
            ..Default::default()
        }))))
    }

    fn join_expr(join: Join) -> SExpr {
        SExpr::create_binary(Arc::new(RelOperator::Join(join)), scan(&[0]), scan(&[1]))
    }

    #[test]
    fn test_finalize_spatial_join_sets_candidate() -> Result<()> {
        let spatial_predicate = function_call("st_intersects", vec![
            column(0, DataType::Geometry),
            column(1, DataType::Geometry),
        ]);
        let join = Join {
            join_type: JoinType::Inner,
            non_equi_conditions: vec![spatial_predicate.clone()],
            ..Default::default()
        };

        let result = FinalizeSpatialJoinOptimizer::new().optimize_sync(&join_expr(join))?;
        let RelOperator::Join(join) = result.plan() else {
            unreachable!()
        };
        let candidate = join.spatial_join.as_ref().expect("spatial candidate");
        assert_eq!(candidate.predicate, spatial_predicate);
        assert_eq!(candidate.left_geometry, column(0, DataType::Geometry));
        assert_eq!(candidate.right_geometry, column(1, DataType::Geometry));

        Ok(())
    }

    #[test]
    fn test_finalize_spatial_join_clears_stale_candidate() -> Result<()> {
        let spatial_predicate = function_call("st_intersects", vec![
            column(0, DataType::Geometry),
            column(1, DataType::Geometry),
        ]);
        let mut join = Join {
            join_type: JoinType::Inner,
            non_equi_conditions: vec![spatial_predicate.clone()],
            ..Default::default()
        };
        let mut optimizer = FinalizeSpatialJoinOptimizer::new();
        let result = optimizer.optimize_sync(&join_expr(join.clone()))?;
        let RelOperator::Join(finalized_join) = result.plan() else {
            unreachable!()
        };
        join.spatial_join = finalized_join.spatial_join.clone();
        join.join_type = JoinType::Left;

        let result = optimizer.optimize_sync(&join_expr(join))?;
        let RelOperator::Join(join) = result.plan() else {
            unreachable!()
        };
        assert!(join.spatial_join.is_none());

        Ok(())
    }
}
