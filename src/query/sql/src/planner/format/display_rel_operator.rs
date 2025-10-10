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

use databend_common_ast::ast::FormatTreeNode;
use itertools::Itertools;

use super::display::IdHumanizer;
use super::display::OperatorHumanizer;
use crate::planner::format::display::DefaultOperatorHumanizer;
use crate::plans::Aggregate;
use crate::plans::AsyncFunction;
use crate::plans::ConstantTableScan;
use crate::plans::EvalScalar;
use crate::plans::Exchange;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::Limit;
use crate::plans::Mutation;
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Scan;
use crate::plans::SecureFilter;
use crate::plans::Sort;
use crate::plans::Udf;
use crate::plans::UnionAll;
use crate::plans::Window;

impl<I: IdHumanizer> OperatorHumanizer<I> for DefaultOperatorHumanizer {
    fn humanize_operator(&self, id_humanizer: &I, op: &RelOperator) -> FormatTreeNode {
        to_format_tree(id_humanizer, op)
    }
}

/// Build `FormatTreeNode` for a `RelOperator`, which may returns a tree structure instead of
/// a single node.
fn to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &RelOperator) -> FormatTreeNode {
    match op {
        RelOperator::Join(op) => join_to_format_tree(id_humanizer, op),
        RelOperator::Scan(op) => scan_to_format_tree(id_humanizer, op),
        RelOperator::EvalScalar(op) => eval_scalar_to_format_tree(id_humanizer, op),
        RelOperator::Filter(op) => filter_to_format_tree(id_humanizer, op),
        RelOperator::SecureFilter(op) => secure_filter_to_format_tree(id_humanizer, op),
        RelOperator::Aggregate(op) => aggregate_to_format_tree(id_humanizer, op),
        RelOperator::Window(op) => window_to_format_tree(id_humanizer, op),
        RelOperator::Udf(op) => udf_to_format_tree(id_humanizer, op),
        RelOperator::AsyncFunction(op) => async_func_to_format_tree(id_humanizer, op),
        RelOperator::Sort(op) => sort_to_format_tree(id_humanizer, op),
        RelOperator::Limit(op) => limit_to_format_tree(id_humanizer, op),
        RelOperator::Exchange(op) => exchange_to_format_tree(id_humanizer, op),
        RelOperator::ConstantTableScan(op) => constant_scan_to_format_tree(id_humanizer, op),
        RelOperator::UnionAll(op) => union_all_to_format_tree(id_humanizer, op),
        RelOperator::Mutation(op) => merge_into_to_format_tree(id_humanizer, op),
        _ => FormatTreeNode::with_children(format!("{:?}", op), vec![]),
    }
}

fn format_scalar<I: IdHumanizer>(_id_humanizer: &I, scalar: &ScalarExpr) -> String {
    match scalar {
        ScalarExpr::BoundColumnRef(column_ref) => {
            if let Some(table_name) = &column_ref.column.table_name {
                format!(
                    "{}.{} (#{})",
                    table_name, column_ref.column.column_name, column_ref.column.index
                )
            } else {
                format!(
                    "{} (#{})",
                    column_ref.column.column_name, column_ref.column.index
                )
            }
        }
        ScalarExpr::ConstantExpr(constant) => constant.value.to_string(),
        ScalarExpr::TypedConstantExpr(constant, _) => constant.value.to_string(),
        ScalarExpr::WindowFunction(win) => win.display_name.clone(),
        ScalarExpr::AggregateFunction(agg) => agg.display_name.clone(),
        ScalarExpr::LambdaFunction(lambda) => {
            let args = lambda
                .args
                .iter()
                .map(|arg| format_scalar(_id_humanizer, arg))
                .collect::<Vec<String>>()
                .join(", ");
            format!(
                "{}({}, {})",
                &lambda.func_name, args, &lambda.lambda_display,
            )
        }
        ScalarExpr::FunctionCall(func) => {
            format!(
                "{}({})",
                &func.func_name,
                func.arguments
                    .iter()
                    .map(|arg| format_scalar(_id_humanizer, arg))
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        }
        ScalarExpr::CastExpr(cast) => {
            format!(
                "CAST({} AS {})",
                format_scalar(_id_humanizer, &cast.argument),
                cast.target_type
            )
        }
        ScalarExpr::SubqueryExpr(_) => "SUBQUERY".to_string(),
        ScalarExpr::UDFCall(udf) => {
            format!(
                "{}({})",
                &udf.handler,
                udf.arguments
                    .iter()
                    .map(|arg| format_scalar(_id_humanizer, arg))
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        }
        ScalarExpr::UDFLambdaCall(udf) => {
            format!(
                "{}({})",
                &udf.func_name,
                format_scalar(_id_humanizer, &udf.scalar)
            )
        }
        ScalarExpr::UDAFCall(udaf) => udaf.display_name.clone(),
        ScalarExpr::AsyncFunctionCall(async_func) => async_func.display_name.clone(),
    }
}

fn format_scalar_item<I: IdHumanizer>(id_humanizer: &I, item: &ScalarItem) -> String {
    format!(
        "{} AS (#{})",
        format_scalar(id_humanizer, &item.scalar),
        item.index
    )
}

fn scan_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &Scan) -> FormatTreeNode {
    FormatTreeNode::with_children("Scan".to_string(), vec![
        FormatTreeNode::new(format!(
            "table: {}",
            id_humanizer.humanize_table_id(op.table_index)
        )),
        FormatTreeNode::new(format!(
            "filters: [{}]",
            op.push_down_predicates
                .iter()
                .flatten()
                .map(|expr| format_scalar(id_humanizer, expr))
                .join(", ")
        )),
        FormatTreeNode::new(format!(
            "order by: [{}]",
            op.order_by.as_ref().map_or_else(
                || "".to_string(),
                |items| items
                    .iter()
                    .map(|item| format!(
                        "{} {}",
                        id_humanizer.humanize_column_id(item.index),
                        if item.asc { "ASC" } else { "DESC" }
                    ))
                    .collect::<Vec<String>>()
                    .join(", "),
            ),
        )),
        FormatTreeNode::new(format!(
            "limit: {}",
            op.limit.map_or("NONE".to_string(), |l| l.to_string())
        )),
    ])
}

fn join_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &Join) -> FormatTreeNode {
    let build_keys = op
        .equi_conditions
        .iter()
        .map(|condition| format_scalar(id_humanizer, &condition.right))
        .collect::<Vec<String>>()
        .join(", ");
    let probe_keys = op
        .equi_conditions
        .iter()
        .map(|condition| format_scalar(id_humanizer, &condition.left))
        .collect::<Vec<String>>()
        .join(", ");
    let join_filters = op
        .non_equi_conditions
        .iter()
        .map(|expr| format_scalar(id_humanizer, expr))
        .collect::<Vec<String>>()
        .join(", ");

    FormatTreeNode::with_children(format!("Join({:?})", op.join_type), vec![
        FormatTreeNode::new(format!("build keys: [{}]", build_keys)),
        FormatTreeNode::new(format!("probe keys: [{}]", probe_keys)),
        FormatTreeNode::new(format!("other filters: [{}]", join_filters)),
    ])
}

fn aggregate_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &Aggregate) -> FormatTreeNode {
    let group_items = op
        .group_items
        .iter()
        .map(|item| format_scalar_item(id_humanizer, item))
        .collect::<Vec<String>>()
        .join(", ");
    let agg_funcs = op
        .aggregate_functions
        .iter()
        .map(|item| format_scalar_item(id_humanizer, item))
        .collect::<Vec<String>>()
        .join(", ");
    FormatTreeNode::with_children(format!("Aggregate({:?})", op.mode), vec![
        FormatTreeNode::new(format!("group items: [{}]", group_items)),
        FormatTreeNode::new(format!("aggregate functions: [{}]", agg_funcs)),
    ])
}

fn window_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &Window) -> FormatTreeNode {
    let partition_by_items = op
        .partition_by
        .iter()
        .map(|item| format_scalar_item(id_humanizer, item))
        .collect::<Vec<String>>()
        .join(", ");

    let order_by_items = op
        .order_by
        .iter()
        .map(|item| {
            format!(
                "{}{}{}",
                format_scalar_item(id_humanizer, &item.order_by_item),
                match &item.asc {
                    Some(true) => " ASC",
                    Some(false) => " DESC",
                    None => "",
                },
                match &item.nulls_first {
                    Some(true) => " NULLS FIRST",
                    Some(false) => " NULLS LAST",
                    None => "",
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let frame = op.frame.to_string();

    FormatTreeNode::with_children("Window".to_string(), vec![
        FormatTreeNode::new(format!("aggregate function: {}", op.function.func_name())),
        FormatTreeNode::new(format!("partition items: [{}]", partition_by_items)),
        FormatTreeNode::new(format!("order by items: [{}]", order_by_items)),
        FormatTreeNode::new(format!("frame: [{}]", frame)),
    ])
}

fn udf_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &Udf) -> FormatTreeNode {
    let scalars = op
        .items
        .iter()
        .sorted_by(|a, b| a.index.cmp(&b.index))
        .map(|item| format_scalar_item(id_humanizer, item))
        .collect::<Vec<String>>()
        .join(", ");
    let name = if op.script_udf {
        "UdfScript".to_string()
    } else {
        "UdfServer".to_string()
    };
    FormatTreeNode::with_children(name, vec![FormatTreeNode::new(format!(
        "scalars: [{}]",
        scalars
    ))])
}

fn async_func_to_format_tree<I: IdHumanizer>(
    id_humanizer: &I,
    op: &AsyncFunction,
) -> FormatTreeNode {
    let scalars = op
        .items
        .iter()
        .sorted_by(|a, b| a.index.cmp(&b.index))
        .map(|item| format_scalar_item(id_humanizer, item))
        .collect::<Vec<String>>()
        .join(", ");
    FormatTreeNode::with_children("AsyncFunction".to_string(), vec![FormatTreeNode::new(
        format!("scalars: [{}]", scalars),
    )])
}

fn filter_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &Filter) -> FormatTreeNode {
    let scalars = op
        .predicates
        .iter()
        .map(|expr| format_scalar(id_humanizer, expr))
        .join(", ");
    FormatTreeNode::with_children("Filter".to_string(), vec![FormatTreeNode::new(format!(
        "filters: [{}]",
        scalars
    ))])
}

fn secure_filter_to_format_tree<I: IdHumanizer>(
    id_humanizer: &I,
    op: &SecureFilter,
) -> FormatTreeNode {
    let scalars = op
        .predicates
        .iter()
        .map(|expr| format_scalar(id_humanizer, expr))
        .join(", ");
    FormatTreeNode::with_children("SecureFilter".to_string(), vec![FormatTreeNode::new(
        format!("secure filters: [{}]", scalars),
    )])
}

fn eval_scalar_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &EvalScalar) -> FormatTreeNode {
    let scalars = op
        .items
        .iter()
        .sorted_by(|a, b| a.index.cmp(&b.index))
        .map(|item| format_scalar_item(id_humanizer, item))
        .collect::<Vec<String>>()
        .join(", ");
    FormatTreeNode::with_children("EvalScalar".to_string(), vec![FormatTreeNode::new(
        format!("scalars: [{}]", scalars),
    )])
}

fn sort_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &Sort) -> FormatTreeNode {
    let scalars = op
        .items
        .iter()
        .map(|item| {
            format!(
                "{} {} NULLS {}",
                id_humanizer.humanize_column_id(item.index),
                if item.asc { "ASC" } else { "DESC" },
                if item.nulls_first { "FIRST" } else { "LAST" }
            )
        })
        .collect::<Vec<String>>()
        .join(", ");
    let limit = op.limit.map_or("NONE".to_string(), |l| l.to_string());

    let children = match &op.window_partition {
        Some(window) => vec![
            FormatTreeNode::new(format!("sort keys: [{}]", scalars)),
            FormatTreeNode::new(format!("limit: [{}]", limit)),
            FormatTreeNode::new(format!(
                "window top: {}",
                window.top.map_or("NONE".to_string(), |n| n.to_string())
            )),
            FormatTreeNode::new(format!("window function: {:?}", window.func)),
        ],
        None => vec![
            FormatTreeNode::new(format!("sort keys: [{}]", scalars)),
            FormatTreeNode::new(format!("limit: [{}]", limit)),
        ],
    };

    FormatTreeNode::with_children("Sort".to_string(), children)
}

fn constant_scan_to_format_tree<I: IdHumanizer>(
    id_humanizer: &I,
    plan: &ConstantTableScan,
) -> FormatTreeNode {
    if plan.num_rows == 0 {
        return FormatTreeNode::new(plan.name().to_string());
    }

    FormatTreeNode::with_children(plan.name().to_string(), vec![
        FormatTreeNode::new(format!(
            "columns: [{}]",
            plan.columns
                .iter()
                .map(|id| id_humanizer.humanize_column_id(*id))
                .join(", ")
        )),
        FormatTreeNode::new(format!("num_rows: [{}]", plan.num_rows)),
    ])
}

fn limit_to_format_tree<I: IdHumanizer>(_: &I, op: &Limit) -> FormatTreeNode {
    let limit = op.limit.unwrap_or_default();
    FormatTreeNode::with_children("Limit".to_string(), vec![
        FormatTreeNode::new(format!("limit: [{}]", limit)),
        FormatTreeNode::new(format!("offset: [{}]", op.offset)),
    ])
}

fn exchange_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &Exchange) -> FormatTreeNode {
    let payload = match op {
        Exchange::Hash(_) => "Exchange(Hash)",
        Exchange::Broadcast => "Exchange(Broadcast)",
        Exchange::Merge => "Exchange(Merge)",
        Exchange::MergeSort => "Exchange(MergeSort)",
    };

    match op {
        Exchange::Hash(keys) => {
            FormatTreeNode::with_children(payload.to_string(), vec![FormatTreeNode::new(format!(
                "Exchange(Hash): keys: [{}]",
                keys.iter()
                    .map(|expr| format_scalar(id_humanizer, expr))
                    .join(", ")
            ))])
        }
        _ => FormatTreeNode::with_children(payload.to_string(), vec![]),
    }
}

fn union_all_to_format_tree<I: IdHumanizer>(id_humanizer: &I, op: &UnionAll) -> FormatTreeNode {
    let children = vec![
        FormatTreeNode::new(format!(
            "output: [{}]",
            op.output_indexes
                .iter()
                .map(|i| id_humanizer.humanize_column_id(*i))
                .join(", ")
        )),
        FormatTreeNode::new(format!(
            "left: [{}]",
            op.left_outputs
                .iter()
                .map(|(i, expr)| {
                    if let Some(expr) = expr {
                        format_scalar(id_humanizer, expr)
                    } else {
                        id_humanizer.humanize_column_id(*i)
                    }
                })
                .join(", ")
        )),
        FormatTreeNode::new(format!(
            "right: [{}]",
            op.right_outputs
                .iter()
                .map(|(i, expr)| {
                    if let Some(expr) = expr {
                        format_scalar(id_humanizer, expr)
                    } else {
                        id_humanizer.humanize_column_id(*i)
                    }
                })
                .join(", ")
        )),
        FormatTreeNode::new(format!("cte_scan_names: {:?}", op.cte_scan_names)),
    ];

    FormatTreeNode::with_children(format!("{:?}", op.rel_op()), children)
}

fn merge_into_to_format_tree<I: IdHumanizer>(
    id_humanizer: &I,
    merge_into: &Mutation,
) -> FormatTreeNode {
    // add merge into target_table
    let table_index = merge_into
        .metadata
        .read()
        .get_table_index(
            Some(merge_into.database_name.as_str()),
            merge_into.table_name.as_str(),
        )
        .unwrap();

    let table_entry = merge_into.metadata.read().table(table_index).clone();
    let target_table_format = format!(
        "target_table: {}.{}.{}",
        table_entry.catalog(),
        table_entry.database(),
        table_entry.name(),
    );

    let target_build_optimization = false;
    let target_build_optimization_format = FormatTreeNode::new(format!(
        "target_build_optimization: {}",
        target_build_optimization
    ));
    let distributed_format =
        FormatTreeNode::new(format!("distributed: {}", merge_into.distributed));
    let can_try_update_column_only_format = FormatTreeNode::new(format!(
        "can_try_update_column_only: {}",
        merge_into.can_try_update_column_only
    ));
    // add matched clauses
    let mut matched_children = Vec::with_capacity(merge_into.matched_evaluators.len());
    let target = table_entry.table().schema_with_stream();
    for evaluator in &merge_into.matched_evaluators {
        let condition_format = evaluator.condition.as_ref().map_or_else(
            || "condition: None".to_string(),
            |predicate| format!("condition: {}", format_scalar(id_humanizer, predicate)),
        );
        if evaluator.update.is_none() {
            matched_children.push(FormatTreeNode::new(format!(
                "matched delete: [{}]",
                condition_format
            )));
        } else {
            let map = evaluator.update.as_ref().unwrap();
            let mut field_indexes: Vec<usize> =
                map.iter().map(|(field_idx, _)| *field_idx).collect();
            field_indexes.sort();
            let update_format = field_indexes
                .iter()
                .map(|field_idx| {
                    let expr = map.get(field_idx).unwrap();
                    format!(
                        "{} = {}",
                        target.field(*field_idx).name(),
                        format_scalar(id_humanizer, expr)
                    )
                })
                .join(",");
            matched_children.push(FormatTreeNode::new(format!(
                "matched update: [{},update set {}]",
                condition_format, update_format
            )));
        }
    }
    // add unmatched clauses
    let mut unmatched_children = Vec::with_capacity(merge_into.unmatched_evaluators.len());
    for evaluator in &merge_into.unmatched_evaluators {
        let condition_format = evaluator.condition.as_ref().map_or_else(
            || "condition: None".to_string(),
            |predicate| format!("condition: {}", format_scalar(id_humanizer, predicate)),
        );
        let insert_schema_format = evaluator
            .source_schema
            .fields
            .iter()
            .map(|field| field.name())
            .join(",");
        let values_format = evaluator
            .values
            .iter()
            .map(|v| format_scalar(id_humanizer, v))
            .join(",");
        let unmatched_format = format!(
            "insert into ({}) values({})",
            insert_schema_format, values_format
        );
        unmatched_children.push(FormatTreeNode::new(format!(
            "unmatched insert: [{},{}]",
            condition_format, unmatched_format
        )));
    }
    let all_children = [
        vec![distributed_format],
        vec![target_build_optimization_format],
        vec![can_try_update_column_only_format],
        matched_children,
        unmatched_children,
    ]
    .concat();
    FormatTreeNode::with_children(target_table_format, all_children)
}
