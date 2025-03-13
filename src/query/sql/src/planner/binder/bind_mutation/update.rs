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

use crate::binder::aggregate::AggregateRewriter;
use crate::binder::bind_mutation::bind::Mutation;
use crate::binder::bind_mutation::bind::MutationStrategy;
use crate::binder::bind_mutation::mutation_expression::MutationExpression;
use crate::binder::util::TableIdentifier;
use crate::binder::Binder;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;
use crate::BindContext;
use crate::ScalarExpr;

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
            TableIdentifier::new(self, catalog, database, table, table_alias);

        let target_table_reference = TableReference::Table {
            span: None,
            catalog: catalog.clone(),
            database: database.clone(),
            table: table.clone(),
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
        let RelOperator::Mutation(mutation) = &*s_expr.plan else {
            return Ok(plan);
        };
        let filter_expr = &s_expr.children[0];
        let RelOperator::Filter(_) = &*filter_expr.plan else {
            return Ok(plan);
        };
        let input = &filter_expr.children[0];
        let RelOperator::Join(_) = &*input.plan else {
            return Ok(plan);
        };

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
            .chain(mutation.required_columns.iter().copied())
            .collect::<HashSet<_>>();

        let used_columns = used_columns
            .difference(&fields_bindings.iter().map(|column| column.index).collect())
            .copied()
            .collect::<HashSet<_>>();

        let aggr_columns = used_columns
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
                let mut aggr_func = ScalarExpr::AggregateFunction(AggregateFunction {
                    span: None,
                    func_name: "any".to_string(),
                    distinct: false,
                    params: vec![],
                    args: vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: binding.clone(),
                    })],
                    return_type: binding.data_type.clone(),
                    sort_descs: vec![],
                    display_name: display_name.clone(),
                });

                let mut rewriter =
                    AggregateRewriter::new(&mut mutation.bind_context, self.metadata.clone());
                rewriter.visit(&mut aggr_func).unwrap();

                let new = mutation
                    .bind_context
                    .aggregate_info
                    .get_aggregate_function(&display_name)
                    .unwrap()
                    .index;

                Some((aggr_func, old, new))
            })
            .collect::<Vec<_>>();

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
                for (_, old, new) in &aggr_columns {
                    expr.replace_column(*old, *new)?
                }
            }

            if let Some(update) = &mut eval.update {
                for (_, expr) in update.iter_mut() {
                    for (_, old, new) in &aggr_columns {
                        expr.replace_column(*old, *new)?
                    }
                }
            }
        }

        for (_, column) in mutation.field_index_map.iter_mut() {
            if let Some((_, _, index)) = aggr_columns
                .iter()
                .find(|(_, i, _)| i.to_string() == *column)
            {
                *column = index.to_string()
            };
        }

        mutation.required_columns = Box::new(
            std::iter::once(mutation.row_id_index)
                .chain(aggr_columns.into_iter().map(|(_, _, i)| i))
                .collect(),
        );

        let aggr_expr = self.bind_aggregate(&mut mutation.bind_context, (**filter_expr).clone())?;

        let s_expr = SExpr::create_unary(
            Arc::new(RelOperator::Mutation(mutation)),
            Arc::new(aggr_expr),
        );

        let Plan::DataMutation {
            schema, metadata, ..
        } = plan
        else {
            unreachable!()
        };

        let plan = Plan::DataMutation {
            s_expr: Box::new(s_expr),
            schema,
            metadata,
        };

        Ok(plan)
    }
}
