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
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::VisitorMut;
use crate::ScalarExpr;

/// Derive filter to push down
pub fn try_derive_predicates(
    s_expr: &SExpr,
    join: Join,
    mut left_push_down: Vec<ScalarExpr>,
    mut right_push_down: Vec<ScalarExpr>,
) -> Result<SExpr> {
    let join_expr = s_expr.child(0)?;
    let mut left_child = join_expr.child(0)?.clone();
    let mut right_child = join_expr.child(1)?.clone();

    if join.join_type == JoinType::Inner {
        let mut new_left_push_down = vec![];
        let mut new_right_push_down = vec![];
        for predicate in left_push_down.iter() {
            let used_columns = predicate.used_columns();
            let mut equi_conditions_map = HashMap::new();
            for (idx, left_condition) in join.left_conditions.iter().enumerate() {
                if left_condition.used_columns().len() > 1
                    || !left_condition.used_columns().is_subset(&used_columns)
                {
                    continue;
                }
                equi_conditions_map.insert(left_condition, &join.right_conditions[idx]);
            }
            if used_columns.len() == equi_conditions_map.len() {
                derive_predicate(
                    &mut equi_conditions_map,
                    predicate,
                    &mut new_right_push_down,
                )?;
            }
        }
        for predicate in right_push_down.iter() {
            let used_columns = predicate.used_columns();
            let mut equi_conditions_map = HashMap::new();
            for (idx, right_condition) in join.right_conditions.iter().enumerate() {
                if right_condition.used_columns().len() > 1
                    || !right_condition.used_columns().is_subset(&used_columns)
                {
                    continue;
                }
                equi_conditions_map.insert(right_condition, &join.left_conditions[idx]);
            }
            if used_columns.len() == equi_conditions_map.len() {
                derive_predicate(&mut equi_conditions_map, predicate, &mut new_left_push_down)?;
            }
        }
        left_push_down.extend(new_left_push_down);
        right_push_down.extend(new_right_push_down);
    }

    if !left_push_down.is_empty() {
        left_child = SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: left_push_down,
                }
                .into(),
            ),
            Arc::new(left_child),
        );
    }

    if !right_push_down.is_empty() {
        right_child = SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: right_push_down,
                }
                .into(),
            ),
            Arc::new(right_child),
        );
    }
    Ok(SExpr::create_binary(
        Arc::new(join.into()),
        Arc::new(left_child),
        Arc::new(right_child),
    ))
}

fn derive_predicate(
    equi_conditions_map: &mut HashMap<&ScalarExpr, &ScalarExpr>,
    predicate: &ScalarExpr,
    new_push_down: &mut Vec<ScalarExpr>,
) -> Result<()> {
    let mut replaced_predicate = predicate.clone();
    replace_column(&mut replaced_predicate, equi_conditions_map);
    if &replaced_predicate != predicate {
        new_push_down.push(replaced_predicate);
    }
    Ok(())
}

fn replace_column(
    scalar: &mut ScalarExpr,
    equi_conditions_map: &mut HashMap<&ScalarExpr, &ScalarExpr>,
) {
    struct ReplaceColumn<'a> {
        equi_conditions_map: &'a HashMap<&'a ScalarExpr, &'a ScalarExpr>,
    }

    impl<'a> VisitorMut<'_> for ReplaceColumn<'a> {
        fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
            if let Some(e) = self.equi_conditions_map.get(expr) {
                *expr = (**e).clone();
                return Ok(());
            } else if let ScalarExpr::BoundColumnRef(col) = expr {
                for (key, val) in self.equi_conditions_map.iter() {
                    if let ScalarExpr::BoundColumnRef(key_col) = key {
                        if key_col.column.index == col.column.index {
                            *expr = (**val).clone();
                            return Ok(());
                        }
                    }
                }
            }

            walk_expr_mut(self, expr)
        }
    }

    let mut replace_column = ReplaceColumn {
        equi_conditions_map,
    };

    replace_column.visit(scalar).unwrap();
}
