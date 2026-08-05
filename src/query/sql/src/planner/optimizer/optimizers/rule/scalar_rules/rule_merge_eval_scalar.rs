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

use crate::ColumnSet;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::EvalScalar;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;
use crate::plans::walk_expr_mut;

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

/// Compose lower scalar definitions into upper expressions when doing so does
/// not duplicate a non-trivial computation. Both item lists in a merged
/// EvalScalar are evaluated against the same input, so an unresolved reference
/// to a lower output would change evaluation order.
fn try_merge_eval_scalars(
    up_eval_scalar: &EvalScalar,
    down_eval_scalar: &EvalScalar,
    input_columns: &ColumnSet,
) -> Result<Option<EvalScalar>> {
    let down_output_columns: ColumnSet = down_eval_scalar
        .items
        .iter()
        .map(|item| item.index)
        .collect();

    let mut up_items = Vec::with_capacity(up_eval_scalar.items.len());
    for item in up_eval_scalar.items.iter().filter(|item| {
        // `#x AS #x` over a lower definition of `#x` is an identity layer.
        // Keeping the lower item directly preserves even non-trivial or
        // non-deterministic lower expressions without evaluating them twice.
        !matches!(
            &item.scalar,
            ScalarExpr::BoundColumnRef(column)
                if column.column.index == item.index
                    && down_output_columns.contains(&item.index)
        )
    }) {
        let mut composer = TrivialComposer {
            down_items: &down_eval_scalar.items,
            supported: true,
        };
        let mut scalar = item.scalar.clone();
        composer.visit(&mut scalar)?;
        if !composer.supported {
            return Ok(None);
        }
        up_items.push(ScalarItem {
            scalar,
            index: item.index,
        });
    }

    let used_columns: ColumnSet = up_items
        .iter()
        .flat_map(|item| item.scalar.used_columns())
        .collect();
    if !used_columns.is_subset(input_columns) {
        return Ok(None);
    }

    // An upper definition shadows the lower item with the same output index.
    // All upper references have already been composed against the lower value.
    let up_output_columns: ColumnSet = up_items.iter().map(|item| item.index).collect();
    let items = up_items
        .into_iter()
        .chain(
            down_eval_scalar
                .items
                .iter()
                .filter(|item| !up_output_columns.contains(&item.index))
                .cloned(),
        )
        .collect();

    Ok(Some(EvalScalar { items }))
}

struct TrivialComposer<'a> {
    down_items: &'a [ScalarItem],
    supported: bool,
}

impl<'a> VisitorMut<'a> for TrivialComposer<'_> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::BoundColumnRef(column) = expr
            && let Some(item) = self
                .down_items
                .iter()
                .find(|item| item.index == column.column.index)
        {
            match &item.scalar {
                ScalarExpr::BoundColumnRef(_)
                | ScalarExpr::ConstantExpr(_)
                | ScalarExpr::TypedConstantExpr(_, _) => {
                    *expr = item.scalar.clone();
                }
                _ => self.supported = false,
            }
            return Ok(());
        }
        walk_expr_mut(self, expr)
    }
}

impl Rule for RuleMergeEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let up_eval_scalar: EvalScalar = s_expr.plan().clone().try_into()?;
        let down_eval_scalar: EvalScalar = s_expr.child(0)?.plan().clone().try_into()?;
        let rel_expr = RelExpr::with_s_expr(s_expr.child(0)?);
        let input_prop = rel_expr.derive_relational_prop_child(0)?;
        if let Some(merged) = try_merge_eval_scalars(
            &up_eval_scalar,
            &down_eval_scalar,
            &input_prop.output_columns,
        )? {
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

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Symbol;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;
    use crate::plans::ConstantExpr;
    use crate::plans::FunctionCall;

    fn int_type() -> DataType {
        DataType::Number(NumberDataType::Int64)
    }

    fn column(index: Symbol) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                index.to_string(),
                index,
                Box::new(int_type().wrap_nullable()),
                Visibility::Visible,
            )
            .build(),
        }
        .into()
    }

    fn null_item(index: Symbol) -> ScalarItem {
        ScalarItem {
            scalar: ScalarExpr::TypedConstantExpr(
                ConstantExpr {
                    span: None,
                    value: Scalar::Null,
                },
                int_type().wrap_nullable(),
            ),
            index,
        }
    }

    #[test]
    fn merges_identity_over_lower_definition() -> Result<()> {
        let input = ColumnSet::new();
        let index = Symbol::new(1);
        let down = EvalScalar {
            items: vec![null_item(index)],
        };
        let up = EvalScalar {
            items: vec![ScalarItem {
                scalar: column(index),
                index,
            }],
        };

        let merged = try_merge_eval_scalars(&up, &down, &input)?.unwrap();
        assert_eq!(merged.items.len(), 1);
        assert_eq!(merged.items[0].index, index);
        assert!(matches!(
            merged.items[0].scalar,
            ScalarExpr::TypedConstantExpr(
                ConstantExpr {
                    value: Scalar::Null,
                    ..
                },
                _
            )
        ));
        Ok(())
    }

    #[test]
    fn composes_column_reference_into_upper_expression() -> Result<()> {
        let input_index = Symbol::new(0);
        let lower_index = Symbol::new(1);
        let output_index = Symbol::new(2);
        let input = [input_index].into_iter().collect();
        let down = EvalScalar {
            items: vec![ScalarItem {
                scalar: column(input_index),
                index: lower_index,
            }],
        };
        let up = EvalScalar {
            items: vec![ScalarItem {
                scalar: ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "is_not_null".to_string(),
                    params: vec![],
                    return_type: None,
                    arguments: vec![column(lower_index)],
                }),
                index: output_index,
            }],
        };

        let merged = try_merge_eval_scalars(&up, &down, &input)?.unwrap();
        let ScalarExpr::FunctionCall(function) = &merged.items[0].scalar else {
            panic!("expected the upper function to remain")
        };
        assert!(matches!(
            &function.arguments[0],
            ScalarExpr::BoundColumnRef(column) if column.column.index == input_index
        ));
        assert_eq!(merged.items[1].index, lower_index);
        Ok(())
    }

    #[test]
    fn composes_constant_into_upper_expression() -> Result<()> {
        let input = ColumnSet::new();
        let lower_index = Symbol::new(1);
        let output_index = Symbol::new(2);
        let down = EvalScalar {
            items: vec![null_item(lower_index)],
        };
        let up = EvalScalar {
            items: vec![ScalarItem {
                scalar: ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "is_not_null".to_string(),
                    params: vec![],
                    return_type: None,
                    arguments: vec![column(lower_index)],
                }),
                index: output_index,
            }],
        };

        let merged = try_merge_eval_scalars(&up, &down, &input)?.unwrap();
        let ScalarExpr::FunctionCall(function) = &merged.items[0].scalar else {
            panic!("expected the upper function to remain")
        };
        assert!(matches!(
            function.arguments[0],
            ScalarExpr::TypedConstantExpr(_, _)
        ));
        assert_eq!(merged.items[1].index, lower_index);
        assert!(matches!(
            merged.items[1].scalar,
            ScalarExpr::TypedConstantExpr(
                ConstantExpr {
                    value: Scalar::Null,
                    ..
                },
                _
            )
        ));
        Ok(())
    }

    #[test]
    fn keeps_layers_for_non_trivial_dependency() -> Result<()> {
        let input_index = Symbol::new(0);
        let lower_index = Symbol::new(1);
        let output_index = Symbol::new(2);
        let input = [input_index].into_iter().collect();
        let down = EvalScalar {
            items: vec![ScalarItem {
                scalar: ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "is_not_null".to_string(),
                    params: vec![],
                    return_type: None,
                    arguments: vec![column(input_index)],
                }),
                index: lower_index,
            }],
        };
        let up = EvalScalar {
            items: vec![ScalarItem {
                scalar: column(lower_index),
                index: output_index,
            }],
        };

        assert!(try_merge_eval_scalars(&up, &down, &input)?.is_none());
        Ok(())
    }
}
