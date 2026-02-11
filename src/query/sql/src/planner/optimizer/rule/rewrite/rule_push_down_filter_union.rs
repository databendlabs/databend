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

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::UnionAll;
use crate::plans::VisitorMut;
use crate::IndexType;

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

        // Create a filter which matches union's left child.
        //
        // Note: union output columns are aligned by ordinal and may not share the same column
        // binding as its children. E.g. `SELECT a AS x, a AS y ... UNION ALL ...`.
        // The filter predicates should be rewritten using `left_outputs`.
        let left_index_pairs: HashMap<IndexType, IndexType> = union
            .left_outputs
            .iter()
            .map(|(idx, expr)| {
                let input_index = match expr {
                    None => *idx,
                    Some(ScalarExpr::BoundColumnRef(col_ref)) => col_ref.column.index,
                    Some(ScalarExpr::CastExpr(cast)) => match cast.argument.as_ref() {
                        ScalarExpr::BoundColumnRef(col_ref) => col_ref.column.index,
                        _ => *idx,
                    },
                    _ => *idx,
                };
                (*idx, input_index)
            })
            .collect();
        let left_predicates = filter
            .predicates
            .iter()
            .map(|predicate| replace_column_binding(&left_index_pairs, predicate.clone()))
            .collect::<Result<Vec<_>>>()?;
        let left_filter = Filter {
            predicates: left_predicates,
        };

        // Create a filter which matches union's right child.
        let right_index_pairs: HashMap<IndexType, IndexType> = union
            .left_outputs
            .iter()
            .zip(union.right_outputs.iter())
            .map(|(left, right)| (left.0, right.0))
            .collect();
        let new_predicates = filter
            .predicates
            .iter()
            .map(|predicate| replace_column_binding(&right_index_pairs, predicate.clone()))
            .collect::<Result<Vec<_>>>()?;
        let right_filer = Filter {
            predicates: new_predicates,
        };

        let mut union_left_child = union_s_expr.child(0)?.clone();
        let mut union_right_child = union_s_expr.child(1)?.clone();

        // Add filter to union children
        union_left_child =
            SExpr::create_unary(Arc::new(left_filter.into()), Arc::new(union_left_child));
        union_right_child =
            SExpr::create_unary(Arc::new(right_filer.into()), Arc::new(union_right_child));

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
            if let Some(new_index) = self.index_pairs.get(&column.column.index) {
                // Keep the original column binding attributes (table/database qualifiers, column
                // position, visibility, etc.) to avoid breaking downstream optimizations (e.g.
                // prewhere push down relies on `table_index`).
                column.column.index = *new_index;
            }
            Ok(())
        }
    }

    let mut visitor = ReplaceColumnVisitor { index_pairs };
    visitor.visit(&mut scalar)?;

    Ok(scalar)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;
    use crate::binder::ColumnBindingBuilder;
    use crate::Visibility;

    #[test]
    fn test_replace_column_binding_preserves_qualifiers() -> Result<()> {
        let mut index_pairs = HashMap::default();
        index_pairs.insert(1, 2);

        let column = ColumnBindingBuilder::new(
            "c".to_string(),
            1,
            Box::new(DataType::Number(NumberDataType::Int32)),
            Visibility::Visible,
        )
        .database_name(Some("db".to_string()))
        .table_name(Some("t".to_string()))
        .table_index(Some(42))
        .column_position(Some(7))
        .virtual_computed_expr(Some("vir".to_string()))
        .build();

        let scalar = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });
        let scalar = replace_column_binding(&index_pairs, scalar)?;

        let ScalarExpr::BoundColumnRef(col_ref) = scalar else {
            unreachable!("expected bound column ref")
        };

        assert_eq!(col_ref.column.index, 2);
        assert_eq!(col_ref.column.database_name.as_deref(), Some("db"));
        assert_eq!(col_ref.column.table_name.as_deref(), Some("t"));
        assert_eq!(col_ref.column.table_index, Some(42));
        assert_eq!(col_ref.column.column_position, Some(7));
        assert_eq!(col_ref.column.virtual_computed_expr.as_deref(), Some("vir"));
        Ok(())
    }
}
