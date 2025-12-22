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

use ahash::HashMap;
use databend_common_exception::Result;

use crate::IndexType;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::UnionAll;
use crate::plans::VisitorMut;

// For a union query, it's not allowed to add `filter` after union
// Such as: `(select * from t1 union all select * from t2) where a > 1`, it's invalid.
// However, it's possible to have `filter` after `union` when involved `view`
// Such as: `create view v_t as (select * from t1 union all select * from t2)`.
// Then use the view with filter, `select * from v_t where a > 1`;
// So it'll be efficient to push down `filter` to `union`, reduce the size of data to pull from table.
pub struct RulePushDownFilterUnion {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownFilterUnion {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterUnion,
            // Filter
            //  \
            //   UnionAll
            //     /  \
            //   ...   ...
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::UnionAll,
                    children: vec![Matcher::Leaf, Matcher::Leaf],
                }],
            }],
        }
    }
}

impl Rule for RulePushDownFilterUnion {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let union_s_expr = s_expr.child(0)?;
        let union: UnionAll = union_s_expr.plan().clone().try_into()?;
        if !union.cte_scan_names.is_empty() {
            // If the union has cte scan names, it's not allowed to push down filter.
            state.add_result(s_expr.clone());
            return Ok(());
        }

        let mut union_left_child = union_s_expr.child(0)?.clone();
        let mut union_right_child = union_s_expr.child(1)?.clone();

        // Add filter to union children
        for (union_side, union_sexpr) in [&union.left_outputs, &union.right_outputs]
            .iter()
            .zip([&mut union_left_child, &mut union_right_child].iter_mut())
        {
            // Create a filter which matches union's right child.
            let index_pairs: HashMap<IndexType, IndexType> = union
                .output_indexes
                .iter()
                .zip(union_side.iter())
                .map(|(index, side)| (*index, side.0))
                .collect();

            let new_predicates = filter
                .predicates
                .iter()
                .map(|predicate| replace_column_binding(&index_pairs, predicate.clone()))
                .collect::<Result<Vec<_>>>()?;

            let filter = Filter {
                predicates: new_predicates,
            };

            let s = (*union_sexpr).clone();
            **union_sexpr = SExpr::create_unary(Arc::new(filter.into()), Arc::new(s));
        }

        // Create a filter which matches union's right child.
        let result = SExpr::create_binary(
            Arc::new(union.into()),
            Arc::new(union_left_child),
            Arc::new(union_right_child),
        );
        state.add_result(result);

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

fn replace_column_binding(
    index_pairs: &HashMap<IndexType, IndexType>,
    mut scalar: ScalarExpr,
) -> Result<ScalarExpr> {
    struct ReplaceColumnVisitor<'a> {
        index_pairs: &'a HashMap<IndexType, IndexType>,
    }

    impl<'a> VisitorMut<'a> for ReplaceColumnVisitor<'a> {
        fn visit_bound_column_ref(&mut self, column: &mut BoundColumnRef) -> Result<()> {
            let index = column.column.index;
            if self.index_pairs.contains_key(&index) {
                let new_column = ColumnBindingBuilder::new(
                    column.column.column_name.clone(),
                    *self.index_pairs.get(&index).unwrap(),
                    column.column.data_type.clone(),
                    Visibility::Visible,
                )
                .virtual_expr(column.column.virtual_expr.clone())
                .build();
                column.column = new_column;
            }
            Ok(())
        }
    }

    let mut visitor = ReplaceColumnVisitor { index_pairs };
    visitor.visit(&mut scalar)?;

    Ok(scalar)
}

impl Default for RulePushDownFilterUnion {
    fn default() -> Self {
        Self::new()
    }
}
