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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::MatchOperation;
use databend_common_ast::ast::MatchedClause;
use databend_common_ast::ast::MutationUpdateExpr;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UpdateStmt;
use databend_common_exception::Result;

use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::IndexType;
use crate::ScalarExpr;
use crate::Visibility;
use crate::binder::Binder;
use crate::binder::aggregate::AggregateRewriter;
use crate::binder::bind_mutation::bind::Mutation;
use crate::binder::bind_mutation::bind::MutationStrategy;
use crate::binder::bind_mutation::mutation_expression::MutationExpression;
use crate::binder::util::TableIdentifier;
use crate::optimizer::ir::Matcher;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::Plan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_update(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &UpdateStmt,
    ) -> Result<Plan> {
        let UpdateStmt {
            catalog,
            database,
            table,
            table_alias,
            update_list,
            from,
            selection,
            with,
            ..
        } = stmt;

        self.init_cte(bind_context, with)?;

        let target_table_identifier =
            TableIdentifier::new(self, catalog, database, table, &None, table_alias);

        let target_table_reference = TableReference::Table {
            span: None,
            catalog: catalog.clone(),
            database: database.clone(),
            table: table.clone(),
            ref_name: None,
            alias: table_alias.clone(),
            temporal: None,
            with_options: None,
            pivot: None,
            unpivot: None,
            sample: None,
        };

        let update_exprs = update_list
            .iter()
            .map(|update_expr| MutationUpdateExpr {
                table: None,
                name: update_expr.name.clone(),
                expr: update_expr.expr.clone(),
            })
            .collect::<Vec<_>>();
        let matched_clause = MatchedClause {
            selection: None,
            operation: MatchOperation::Update {
                update_list: update_exprs,
                is_star: false,
            },
        };

        let from = from.as_ref().map(|from| from.transform_table_reference());
        let mutation = Mutation {
            target_table_identifier,
            expression: MutationExpression::Update {
                target: target_table_reference,
                filter: selection.clone(),
                from,
            },
            strategy: MutationStrategy::MatchedOnly,
            matched_clauses: vec![matched_clause],
            unmatched_clauses: vec![],
        };

        let plan = self.bind_mutation(bind_context, mutation).await?;

        let settings = self.ctx.get_settings();
        if settings.get_error_on_nondeterministic_update()? {
            Ok(plan)
        } else {
            self.rewrite_nondeterministic_update(plan)
        }
    }

    fn rewrite_nondeterministic_update(&mut self, plan: Plan) -> Result<Plan> {
        let Plan::DataMutation { box s_expr, .. } = &plan else {
            return Ok(plan);
        };
        let RelOperator::Mutation(mutation) = s_expr.plan() else {
            return Ok(plan);
        };
        let matcher = Matcher::MatchOp {
            op_type: RelOp::Filter,
            children: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
        };
        if !matcher.matches(s_expr.unary_child()) {
            return Ok(plan);
        }

        let mut mutation = mutation.clone();

        let row_id = mutation
            .bind_context
            .columns
            .iter()
            .find(|binding| binding.index == mutation.row_id_index)
            .unwrap()
            .clone();

        let table_schema = mutation
            .metadata
            .read()
            .table(mutation.target_table_index)
            .table()
            .schema();

        let fields_bindings = table_schema
            .fields
            .iter()
            .filter_map(|field| {
                if field.computed_expr.is_some() {
                    return None;
                }
                mutation
                    .bind_context
                    .columns
                    .iter()
                    .find(|binding| {
                        binding.table_index == Some(mutation.target_table_index)
                            && binding.column_name == field.name
                    })
                    .cloned()
            })
            .collect::<Vec<_>>();

        let used_columns = mutation
            .matched_evaluators
            .iter()
            .flat_map(|eval| {
                eval.update.iter().flat_map(|update| {
                    update
                        .values()
                        .flat_map(|expr| expr.used_columns().into_iter())
                })
            })
            .collect::<HashSet<_>>();

        let used_columns = used_columns
            .difference(&fields_bindings.iter().map(|column| column.index).collect())
            .copied()
            .collect::<HashSet<_>>();

        struct AnyColumn {
            old: IndexType,
            new: IndexType,
            cast: Option<ScalarExpr>,
        }

        let mut any_columns = used_columns
            .iter()
            .copied()
            .filter_map(|i| {
                if i == mutation.row_id_index {
                    return None;
                }

                let binding = mutation
                    .bind_context
                    .columns
                    .iter()
                    .find(|binding| binding.index == i)
                    .cloned()?;

                let display_name = format!("any({})", binding.index);
                let old = binding.index;
                let mut any_func: ScalarExpr = AggregateFunction {
                    span: None,
                    func_name: "any".to_string(),
                    distinct: false,
                    params: vec![],
                    args: vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: binding.clone(),
                    })],
                    return_type: Box::new(binding.data_type.wrap_nullable()),
                    sort_descs: vec![],
                    display_name: display_name.clone(),
                }
                .into();

                let mut rewriter =
                    AggregateRewriter::new(&mut mutation.bind_context, self.metadata.clone());
                rewriter.visit(&mut any_func).unwrap();

                let new = mutation
                    .bind_context
                    .aggregate_info
                    .get_aggregate_function(&display_name)
                    .unwrap()
                    .index;

                let (cast, new) = if !binding.data_type.is_nullable() {
                    let ColumnBinding {
                        column_name,
                        data_type,
                        ..
                    } = binding;

                    let column = ColumnBindingBuilder::new(
                        column_name.clone(),
                        new,
                        data_type.clone(),
                        Visibility::Visible,
                    )
                    .build();
                    let column = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });
                    let cast = column.unify_to_data_type(&data_type);

                    let index = self
                        .metadata
                        .write()
                        .add_derived_column(column_name, *data_type);
                    (Some(cast), index)
                } else {
                    (None, new)
                };

                Some(AnyColumn { old, new, cast })
            })
            .collect::<Vec<_>>();

        let items = any_columns
            .iter_mut()
            .filter_map(|col| {
                col.cast.take().map(|scalar| ScalarItem {
                    scalar,
                    index: col.new,
                })
            })
            .collect();
        let eval_scalar = EvalScalar { items };

        mutation.bind_context.aggregate_info.group_items = fields_bindings
            .into_iter()
            .chain(std::iter::once(row_id))
            .map(|column| ScalarItem {
                index: column.index,
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column }),
            })
            .collect();

        for eval in &mut mutation.matched_evaluators {
            if let Some(expr) = &mut eval.condition {
                for col in &any_columns {
                    expr.replace_column(col.old, col.new)?
                }
            }

            if let Some(update) = &mut eval.update {
                for (_, expr) in update.iter_mut() {
                    for col in &any_columns {
                        expr.replace_column(col.old, col.new)?
                    }
                }
            }
        }

        for (_, column) in mutation.field_index_map.iter_mut() {
            if let Some(col) = any_columns.iter().find(|c| c.old.to_string() == *column) {
                *column = col.new.to_string()
            };
        }

        // update required columns
        for any_column in any_columns {
            mutation.required_columns.remove(&any_column.old);
            mutation.required_columns.insert(any_column.new);
        }

        let aggr_expr =
            self.bind_aggregate(&mut mutation.bind_context, s_expr.unary_child().clone())?;

        let input = if eval_scalar.items.is_empty() {
            aggr_expr
        } else {
            aggr_expr.build_unary(Arc::new(eval_scalar.into()))
        };

        let s_expr = Box::new(input.build_unary(Arc::new(mutation.into())));
        let Plan::DataMutation {
            schema, metadata, ..
        } = plan
        else {
            unreachable!()
        };
        Ok(Plan::DataMutation {
            s_expr,
            schema,
            metadata,
        })
    }
}
