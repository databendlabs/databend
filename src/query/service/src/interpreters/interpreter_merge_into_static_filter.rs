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

use databend_common_ast::parser::parse_comma_separated_exprs;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::Dialect;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::SendableDataBlockStream;
use databend_common_sql::bind_one_table;
use databend_common_sql::format_scalar;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans::Aggregate;
use databend_common_sql::plans::AggregateFunction;
use databend_common_sql::plans::AggregateMode;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::EvalScalar;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarItem;
use databend_common_sql::plans::VisitorMut;
use databend_common_sql::BindContext;
use databend_common_sql::ColumnBinding;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::MetadataRef;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeChecker;
use databend_common_sql::Visibility;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use log::info;
use log::warn;
use tokio_stream::StreamExt;

use super::InterpreterPtr;
use crate::interpreters::interpreter_merge_into::MergeIntoInterpreter;
use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;

struct MergeStyleJoin<'a> {
    source_conditions: &'a [ScalarExpr],
    target_conditions: &'a [ScalarExpr],
    source_sexpr: &'a SExpr,
    target_sexpr: &'a SExpr,
}

impl MergeStyleJoin<'_> {
    pub fn new(join: &SExpr) -> MergeStyleJoin {
        let join_op = match join.plan() {
            RelOperator::Join(j) => j,
            _ => unreachable!(),
        };
        assert!(
            join_op.join_type == JoinType::Right
                || join_op.join_type == JoinType::RightAnti
                || join_op.join_type == JoinType::Inner
        );
        let source_conditions = &join_op.right_conditions;
        let target_conditions = &join_op.left_conditions;
        let source_sexpr = join.child(1).unwrap();
        let target_sexpr = join.child(0).unwrap();
        MergeStyleJoin {
            source_conditions,
            target_conditions,
            source_sexpr,
            target_sexpr,
        }
    }

    pub fn collect_column_map(&self) -> HashMap<String, ColumnBinding> {
        let mut column_map = HashMap::new();
        for (t, s) in self
            .target_conditions
            .iter()
            .zip(self.source_conditions.iter())
        {
            if let (ScalarExpr::BoundColumnRef(t_col), ScalarExpr::BoundColumnRef(s_col)) = (t, s) {
                column_map.insert(t_col.column.column_name.clone(), s_col.column.clone());
            }
        }
        column_map
    }
}

impl MergeIntoInterpreter {
    pub async fn build_static_filter(
        join: &SExpr,
        metadata: &MetadataRef,
        ctx: Arc<QueryContext>,
        table: Arc<dyn Table>,
    ) -> Result<Box<SExpr>> {
        // 1. collect statistics from the source side
        // plan of source table is extended to:
        //
        // AggregateFinal(min(source_join_side_expr),max(source_join_side_expr))
        //        \
        //     AggregatePartial(min(source_join_side_expr),max(source_join_side_expr))
        //         \
        //       EvalScalar(source_join_side_expr)
        //          \
        //         SourcePlan
        let m_join = MergeStyleJoin::new(join);
        if m_join.source_conditions.is_empty() {
            return Ok(Box::new(join.clone()));
        }
        let column_map = m_join.collect_column_map();

        let fuse_table = table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support MERGE INTO",
                table.name(),
                table.get_table_info().engine(),
            ))
        })?;

        let group_expr = match fuse_table.cluster_key_str() {
            None => {
                info!("no cluster key found, use default plan");
                return Ok(Box::new(join.clone()));
            }
            Some(cluster_key_str) => {
                match Self::extract_group_by_expr(&ctx, table.clone(), cluster_key_str, &column_map)
                    .await?
                {
                    None => {
                        warn!(
                            "no suitable group by expr found, use default plan. cluster_key_str is '{}'",
                            cluster_key_str
                        );
                        return Ok(Box::new(join.clone()));
                    }
                    Some(expr) => expr,
                }
            }
        };

        ctx.set_status_info("constructing static filter plan");
        let plan = Self::build_min_max_group_by_left_most_cluster_key_expr_plan(
            &m_join, metadata, group_expr,
        )
        .await?;

        ctx.set_status_info("executing static filter plan");
        let interpreter: InterpreterPtr = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream: SendableDataBlockStream = interpreter.execute(ctx.clone()).await?;
        let blocks = stream.collect::<Result<Vec<_>>>().await?;
        // check if number of partitions is too much
        {
            let max_number_partitions =
                ctx.get_settings()
                    .get_merge_into_static_filter_partition_threshold()? as usize;

            let number_partitions: usize = blocks.iter().map(|b| b.num_rows()).sum();
            if number_partitions > max_number_partitions {
                warn!(
                    "number of partitions {} exceeds threshold {}",
                    number_partitions, max_number_partitions
                );
                return Ok(Box::new(join.clone()));
            }
        }

        // 2. build filter and push down to target side
        ctx.set_status_info("building pushdown filters");
        let mut filters = Vec::with_capacity(m_join.target_conditions.len());

        for (i, target_side_expr) in m_join.target_conditions.iter().enumerate() {
            let mut filter_parts = vec![];
            for block in blocks.iter() {
                let block = block.convert_to_full();
                let min_column = block.get_by_offset(i * 2).value.as_column().unwrap();
                let max_column = block.get_by_offset(i * 2 + 1).value.as_column().unwrap();
                for (min_scalar, max_scalar) in min_column.iter().zip(max_column.iter()) {
                    let gte_min = ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "gte".to_string(),
                        params: vec![],
                        arguments: vec![
                            target_side_expr.clone(),
                            ScalarExpr::ConstantExpr(ConstantExpr {
                                span: None,
                                value: min_scalar.to_owned(),
                            }),
                        ],
                    });
                    let lte_max = ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "lte".to_string(),
                        params: vec![],
                        arguments: vec![
                            target_side_expr.clone(),
                            ScalarExpr::ConstantExpr(ConstantExpr {
                                span: None,
                                value: max_scalar.to_owned(),
                            }),
                        ],
                    });
                    let and = ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and".to_string(),
                        params: vec![],
                        arguments: vec![gte_min, lte_max],
                    });
                    filter_parts.push(and);
                }
            }
            filters.extend(Self::combine_filter_parts(&filter_parts).into_iter());
        }
        let mut target_plan = m_join.target_sexpr.clone();
        Self::push_down_filters(&mut target_plan, &filters)?;
        let source_plan = m_join.source_sexpr;
        let new_sexpr =
            join.replace_children(vec![Arc::new(target_plan), Arc::new(source_plan.clone())]);

        ctx.set_status_info("join expression replaced");
        Ok(Box::new(new_sexpr))
    }

    fn combine_filter_parts(filter_parts: &[ScalarExpr]) -> Option<ScalarExpr> {
        match filter_parts.len() {
            0 => None,
            1 => Some(filter_parts[0].clone()),
            _ => {
                let mid = filter_parts.len() / 2;
                let left = Self::combine_filter_parts(&filter_parts[0..mid]);
                let right = Self::combine_filter_parts(&filter_parts[mid..]);
                if let Some(left) = left {
                    if let Some(right) = right {
                        Some(ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            func_name: "or".to_string(),
                            params: vec![],
                            arguments: vec![left, right],
                        }))
                    } else {
                        Some(left)
                    }
                } else {
                    right
                }
            }
        }
    }

    fn push_down_filters(s_expr: &mut SExpr, filters: &[ScalarExpr]) -> Result<()> {
        match s_expr.plan() {
            RelOperator::Scan(s) => {
                let mut new_scan = s.clone();
                info!("push down {} filters:", filters.len());
                for filter in filters {
                    info!("{}", format_scalar(filter));
                }
                if let Some(preds) = new_scan.push_down_predicates {
                    new_scan.push_down_predicates =
                        Some(preds.iter().chain(filters).cloned().collect());
                } else {
                    new_scan.push_down_predicates = Some(filters.to_vec());
                }
                *s_expr = SExpr::create_leaf(Arc::new(RelOperator::Scan(new_scan)));
            }
            RelOperator::EvalScalar(_)
            | RelOperator::Filter(_)
            | RelOperator::Aggregate(_)
            | RelOperator::Sort(_)
            | RelOperator::Limit(_) => {
                let mut new_child = s_expr.child(0)?.clone();
                Self::push_down_filters(&mut new_child, filters)?;
                *s_expr = s_expr.replace_children(vec![Arc::new(new_child)]);
            }
            RelOperator::CteScan(_) => {}
            RelOperator::Join(_) => {}
            RelOperator::Exchange(_) => {}
            RelOperator::UnionAll(_) => {}
            RelOperator::DummyTableScan(_) => {}
            RelOperator::Window(_) => {}
            RelOperator::ProjectSet(_) => {}
            RelOperator::MaterializedCte(_) => {}
            RelOperator::ConstantTableScan(_) => {}
            RelOperator::Pattern(_) => {}
            RelOperator::AddRowNumber(_) => {}
            RelOperator::Udf(_) => {}
        }
        Ok(())
    }

    // Extract group by expr from cluster key
    //
    // the left most cluster key expression will be returned if any,
    // otherwise None will be returned.
    async fn extract_group_by_expr(
        ctx: &Arc<QueryContext>,
        table: Arc<dyn Table>,
        cluster_key_str: &str,
        column_map: &HashMap<String, ColumnBinding>,
    ) -> Result<Option<ScalarExpr>> {
        let ast_exprs = {
            let sql_dialect = Dialect::MySQL;
            let tokens = tokenize_sql(cluster_key_str)?;
            parse_comma_separated_exprs(&tokens, sql_dialect)?
        };

        let ast_expr = if !ast_exprs.is_empty() {
            &ast_exprs[0]
        } else {
            warn!("empty cluster key found after parsing.");
            return Ok(None);
        };

        let (mut bind_context, metadata) = bind_one_table(table)?;
        let name_resolution_ctx = NameResolutionContext::try_from(ctx.get_settings().as_ref())?;
        let mut type_checker = {
            let forbid_udf = true;
            TypeChecker::try_create(
                &mut bind_context,
                ctx.clone(),
                &name_resolution_ctx,
                metadata.clone(),
                &[],
                forbid_udf,
            )?
        };

        let (scalar_expr, _) = *type_checker.resolve(ast_expr).await?;

        let mut left_most_expr = match &scalar_expr {
            ScalarExpr::FunctionCall(f) if f.func_name == "tuple" && !f.arguments.is_empty() => {
                f.arguments[0].clone()
            }
            ScalarExpr::FunctionCall(_) => {
                warn!("cluster key expr is not a (suitable) tuple expression");
                return Ok(None);
            }
            _ => scalar_expr,
        };

        struct ReplaceColumnVisitor<'a> {
            column_map: &'a HashMap<String, ColumnBinding>,
        }

        impl<'a> VisitorMut<'a> for ReplaceColumnVisitor<'a> {
            fn visit_bound_column_ref(&mut self, column: &mut BoundColumnRef) -> Result<()> {
                if let Some(new_column) = self.column_map.get(&column.column.column_name) {
                    column.column = new_column.clone();
                    Ok(())
                } else {
                    Err(ErrorCode::from_string_no_backtrace(String::new()))
                }
            }
        }

        let mut visitor = ReplaceColumnVisitor { column_map };

        if visitor.visit(&mut left_most_expr).is_ok() {
            Ok(Some(left_most_expr))
        } else {
            Ok(None)
        }
    }

    async fn build_min_max_group_by_left_most_cluster_key_expr_plan(
        m_join: &MergeStyleJoin<'_>,
        metadata: &MetadataRef,
        group_expr: ScalarExpr,
    ) -> Result<Plan> {
        let mut eval_scalar_items = Vec::with_capacity(m_join.source_conditions.len());
        let mut min_max_binding = Vec::with_capacity(m_join.source_conditions.len() * 2);
        let mut min_max_scalar_items = Vec::with_capacity(m_join.source_conditions.len() * 2);
        let mut group_items = vec![];

        let index = metadata
            .write()
            .add_derived_column("".to_string(), group_expr.data_type()?);
        let evaled = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "".to_string(),
                index,
                Box::new(group_expr.data_type()?),
                Visibility::Visible,
            )
            .build(),
        });
        eval_scalar_items.push(ScalarItem {
            scalar: group_expr,
            index,
        });
        group_items.push(ScalarItem {
            scalar: evaled,
            index,
        });
        for source_side_expr in m_join.source_conditions {
            // eval source side join expr
            let index = metadata
                .write()
                .add_derived_column("".to_string(), source_side_expr.data_type()?);
            let evaled = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    "".to_string(),
                    index,
                    Box::new(source_side_expr.data_type()?),
                    Visibility::Visible,
                )
                .build(),
            });
            eval_scalar_items.push(ScalarItem {
                scalar: source_side_expr.clone(),
                index,
            });

            // eval min/max of source side join expr
            let min_display_name = format!("min({:?})", source_side_expr);
            let max_display_name = format!("max({:?})", source_side_expr);
            let min_index = metadata
                .write()
                .add_derived_column(min_display_name.clone(), source_side_expr.data_type()?);
            let max_index = metadata
                .write()
                .add_derived_column(max_display_name.clone(), source_side_expr.data_type()?);
            let min_binding = ColumnBindingBuilder::new(
                min_display_name.clone(),
                min_index,
                Box::new(source_side_expr.data_type()?),
                Visibility::Visible,
            )
            .build();
            let max_binding = ColumnBindingBuilder::new(
                max_display_name.clone(),
                max_index,
                Box::new(source_side_expr.data_type()?),
                Visibility::Visible,
            )
            .build();
            min_max_binding.push(min_binding);
            min_max_binding.push(max_binding);
            let min = ScalarItem {
                scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                    func_name: "min".to_string(),
                    distinct: false,
                    params: vec![],
                    args: vec![evaled.clone()],
                    return_type: Box::new(source_side_expr.data_type()?),
                    display_name: min_display_name.clone(),
                }),
                index: min_index,
            };
            let max = ScalarItem {
                scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                    func_name: "max".to_string(),
                    distinct: false,
                    params: vec![],
                    args: vec![evaled],
                    return_type: Box::new(source_side_expr.data_type()?),
                    display_name: max_display_name.clone(),
                }),
                index: max_index,
            };
            min_max_scalar_items.push(min);
            min_max_scalar_items.push(max);
        }

        let eval_source_side_join_expr_op = EvalScalar {
            items: eval_scalar_items,
        };
        let source_plan = m_join.source_sexpr;
        let eval_target_side_condition_sexpr = if let RelOperator::Exchange(_) = source_plan.plan()
        {
            // there is another row_number operator here
            SExpr::create_unary(
                Arc::new(eval_source_side_join_expr_op.into()),
                Arc::new(source_plan.child(0)?.child(0)?.clone()),
            )
        } else {
            SExpr::create_unary(
                Arc::new(eval_source_side_join_expr_op.into()),
                Arc::new(source_plan.clone()),
            )
        };

        let mut bind_context = Box::new(BindContext::new());
        bind_context.columns = min_max_binding;

        let agg_partial_op = Aggregate {
            mode: AggregateMode::Partial,
            group_items: group_items.clone(),
            aggregate_functions: min_max_scalar_items.clone(),
            from_distinct: false,
            limit: None,
            grouping_sets: None,
        };
        let agg_partial_sexpr = SExpr::create_unary(
            Arc::new(agg_partial_op.into()),
            Arc::new(eval_target_side_condition_sexpr),
        );
        let agg_final_op = Aggregate {
            mode: AggregateMode::Final,
            group_items,
            aggregate_functions: min_max_scalar_items,
            from_distinct: false,
            limit: None,
            grouping_sets: None,
        };
        let agg_final_sexpr =
            SExpr::create_unary(Arc::new(agg_final_op.into()), Arc::new(agg_partial_sexpr));
        Ok(Plan::Query {
            s_expr: Box::new(agg_final_sexpr),
            metadata: metadata.clone(),
            bind_context,
            rewrite_kind: None,
            formatted_ast: None,
            ignore_result: false,
        })
    }
}
