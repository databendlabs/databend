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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::walk_expr_mut;
use crate::plans::EvalScalar;
use crate::plans::RelOp;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;

// Merge two adjacent `EvalScalar`s into one
pub struct RuleMergeEvalScalar {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleMergeEvalScalar {
    pub fn new() -> Self {
        Self {
            id: RuleID::MergeEvalScalar,
            // EvalScalar
            // \
            //  EvalScalar
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::Leaf],
                }],
            }],
        }
    }
}

impl Rule for RuleMergeEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let up_eval_scalar: EvalScalar = s_expr.plan().clone().try_into()?;
        let down_eval_scalar: EvalScalar = s_expr.child(0)?.plan().clone().try_into()?;

        let mut used_columns = ColumnSet::new();
        let merged_items = Self::merge_items(up_eval_scalar, down_eval_scalar, &mut used_columns)?;

        let rel_expr = RelExpr::with_s_expr(s_expr.child(0)?);
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Check that all used columns are available
        if used_columns.is_subset(&input_prop.output_columns) {
            // TODO(leiysky): eliminate duplicated scalars
            let merged = EvalScalar {
                items: merged_items,
            };

            let new_expr = SExpr::create_unary(
                Arc::new(merged.into()),
                Arc::new(s_expr.child(0)?.child(0)?.clone()),
            );
            state.add_result(new_expr);
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleMergeEvalScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleMergeEvalScalar {
    fn merge_items(
        up_eval_scalar: EvalScalar,
        down_eval_scalar: EvalScalar,
        used_columns: &mut BTreeSet<IndexType>,
    ) -> Result<Vec<ScalarItem>> {
        let mut replace_set = HashMap::with_capacity(down_eval_scalar.items.len());

        for item in &down_eval_scalar.items {
            replace_set.insert(item.index, item.scalar.clone());
        }

        struct ReplaceColumnVisitor {
            replace_set: HashMap<IndexType, ScalarExpr>,
        }

        impl VisitorMut<'_> for ReplaceColumnVisitor {
            fn visit(&mut self, expr: &'_ mut ScalarExpr) -> Result<()> {
                if let ScalarExpr::BoundColumnRef(column_ref) = expr {
                    if let Some(v) = self.replace_set.get(&column_ref.column.index) {
                        *expr = v.clone();
                    }

                    return Ok(());
                }

                walk_expr_mut(self, expr)
            }
        }

        let mut visitor = ReplaceColumnVisitor { replace_set };

        let mut new_items = down_eval_scalar.items;
        for mut item in up_eval_scalar.items {
            // Skip #X AS #X
            if let ScalarExpr::BoundColumnRef(column_ref) = &item.scalar {
                if column_ref.column.index == item.index
                    && visitor.replace_set.contains_key(&item.index)
                {
                    continue;
                }
            }

            visitor.visit(&mut item.scalar)?;

            *used_columns = used_columns
                .union(&item.scalar.used_columns())
                .cloned()
                .collect();

            new_items.push(item);
        }

        Ok(new_items)
    }
}
