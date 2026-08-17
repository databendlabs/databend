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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use log::info;

use super::view_rewrite::QueryInfo;
use super::view_rewrite::ViewInfo;
use super::view_rewrite::ViewMatcher;
use super::view_rewrite::format_scalar;
use crate::IndexType;
use crate::MaterializedViewCandidate;
use crate::Metadata;
use crate::ScalarExpr;
use crate::Symbol;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::ScalarItem;

pub(crate) fn try_rewrite(
    table_index: IndexType,
    table_name: &str,
    metadata: &Metadata,
    s_expr: &SExpr,
    candidates: &[MaterializedViewCandidate],
    required_output_columns: Option<&HashSet<Symbol>>,
) -> Result<Option<(SExpr, u64)>> {
    let mut query_info = QueryInfo::new(
        table_index,
        table_name,
        metadata.columns_by_table_index(table_index),
        s_expr,
    )?;
    if let Some(required_output_columns) = required_output_columns {
        query_info.retain_output_columns(required_output_columns);
    }
    let query_aggregate = query_info.aggregate.clone();
    let matcher = ViewMatcher::new(query_info).with_aggregate_rollup();

    for candidate in candidates {
        let definition_info = QueryInfo::new(
            table_index,
            table_name,
            metadata.columns_by_table_index(table_index),
            &candidate.definition,
        )?;
        if candidate.definition_output_columns.len() != candidate.read_output_columns.len() {
            continue;
        }

        let mut outputs = HashMap::with_capacity(definition_info.output_cols().len());
        for definition_output in definition_info.output_cols() {
            let Some(position) = candidate
                .definition_output_columns
                .iter()
                .position(|column| *column == definition_output.index)
            else {
                continue;
            };
            let read_output = candidate.read_output_columns[position];
            let display_name =
                format_scalar(&definition_output.scalar, definition_info.column_map());
            let replacement = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    metadata.column(read_output).name(),
                    read_output,
                    Box::new(metadata.column(read_output).data_type()),
                    Visibility::Visible,
                )
                .build(),
            });
            // Aggregate MVs expose finalized logical aggregate values. The
            // first implementation supports only equal-granularity aggregate
            // matching; the shared matcher rejects use as an ordinary scalar.
            let is_aggregate = definition_info.aggregate.as_ref().is_some_and(|aggregate| {
                aggregate
                    .aggregate_functions
                    .iter()
                    .any(|item| item.index == definition_output.index)
            });
            outputs.insert(display_name, (replacement, is_aggregate));
        }

        let view_info = ViewInfo::new(
            table_index,
            table_name,
            metadata.columns_by_table_index(table_index),
            &candidate.definition,
            outputs,
        )?;
        let Some(matched) = matcher.try_match(&view_info)? else {
            continue;
        };
        let post_aggregate_predicates = matched.post_aggregate_predicates.clone();

        if matched.requires_aggregate_rollup {
            let Some(query_aggregate) = &query_aggregate else {
                continue;
            };
            if let Some(mut replacement) =
                try_build_state_rollup(candidate, &matched, query_aggregate)?
            {
                replacement = apply_post_aggregate_filter(replacement, &post_aggregate_predicates);
                info!(
                    "Use materialized view {} with aggregate-state rollup: {}",
                    candidate.mv_table_id, candidate.logical_sql
                );
                return Ok(Some((replacement, candidate.mv_table_id)));
            }
        }

        let mut replacement = candidate.read_plan.clone();
        if !matched.predicates.is_empty() {
            replacement = SExpr::create_unary(
                Arc::new(
                    Filter {
                        predicates: matched.predicates,
                    }
                    .into(),
                ),
                Arc::new(replacement),
            );
        }
        if !matched.selection.is_empty() {
            replacement = SExpr::create_unary(
                Arc::new(
                    EvalScalar {
                        items: matched.selection,
                    }
                    .into(),
                ),
                Arc::new(replacement),
            );
        }
        if matched.requires_aggregate_rollup {
            let Some(query_aggregate) = &query_aggregate else {
                continue;
            };
            let Some(aggregate) = build_rollup_aggregate(metadata, query_aggregate)? else {
                continue;
            };
            replacement = SExpr::create_unary(Arc::new(aggregate.into()), Arc::new(replacement));
        }
        replacement = apply_post_aggregate_filter(replacement, &post_aggregate_predicates);

        info!(
            "Use materialized view {}: {}",
            candidate.mv_table_id, candidate.logical_sql
        );
        return Ok(Some((replacement, candidate.mv_table_id)));
    }

    Ok(None)
}

fn apply_post_aggregate_filter(
    replacement: SExpr,
    post_aggregate_predicates: &[ScalarExpr],
) -> SExpr {
    if post_aggregate_predicates.is_empty() {
        replacement
    } else {
        SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: post_aggregate_predicates.to_vec(),
                }
                .into(),
            ),
            Arc::new(replacement),
        )
    }
}

fn try_build_state_rollup(
    candidate: &MaterializedViewCandidate,
    matched: &super::view_rewrite::ViewRewriteMatch,
    query_aggregate: &Aggregate,
) -> Result<Option<SExpr>> {
    let Some(final_projection) = candidate.read_plan.plan().as_eval_scalar() else {
        return Ok(None);
    };
    let state_merge_expr = candidate.read_plan.unary_child();
    let Some(state_merge) = state_merge_expr.plan().as_aggregate() else {
        return Ok(None);
    };

    let mut logical_to_merged = HashMap::with_capacity(final_projection.items.len());
    for item in &final_projection.items {
        let ScalarExpr::BoundColumnRef(column) = &item.scalar else {
            return Ok(None);
        };
        logical_to_merged.insert(item.index, column.column.clone());
    }

    let mut query_to_logical = HashMap::with_capacity(matched.selection.len());
    for item in &matched.selection {
        let ScalarExpr::BoundColumnRef(column) = &item.scalar else {
            return Ok(None);
        };
        query_to_logical.insert(item.index, column.column.index);
    }

    let mut group_projection = Vec::with_capacity(query_aggregate.group_items.len());
    for query_item in &query_aggregate.group_items {
        let Some(logical_index) = query_to_logical.get(&query_item.index) else {
            return Ok(None);
        };
        let Some(merged_column) = logical_to_merged.get(logical_index) else {
            return Ok(None);
        };
        let Some(merged_group_item) = state_merge
            .group_items
            .iter()
            .find(|item| item.index == merged_column.index)
        else {
            return Ok(None);
        };
        group_projection.push(ScalarItem {
            index: query_item.index,
            scalar: merged_group_item.scalar.clone(),
        });
    }

    let mut aggregate_functions = Vec::with_capacity(query_aggregate.aggregate_functions.len());
    for query_item in &query_aggregate.aggregate_functions {
        let ScalarExpr::AggregateFunction(query_function) = &query_item.scalar else {
            return Ok(None);
        };
        let Some(logical_index) = query_to_logical.get(&query_item.index) else {
            return Ok(None);
        };
        let Some(merged_column) = logical_to_merged.get(logical_index) else {
            return Ok(None);
        };
        let Some(merged_item) = state_merge
            .aggregate_functions
            .iter()
            .find(|item| item.index == merged_column.index)
        else {
            return Ok(None);
        };
        let ScalarExpr::AggregateFunction(merge_function) = &merged_item.scalar else {
            return Ok(None);
        };
        if merge_function.return_type != query_function.return_type {
            return Ok(None);
        }
        let mut merge_function = merge_function.clone();
        merge_function.display_name = query_function.display_name.clone();
        aggregate_functions.push(ScalarItem {
            index: query_item.index,
            scalar: ScalarExpr::AggregateFunction(merge_function),
        });
    }

    let mut child = state_merge_expr.unary_child().clone();
    if !matched.predicates.is_empty() {
        let mut predicates = matched.predicates.clone();
        for predicate in &mut predicates {
            for (logical_index, merged_column) in &logical_to_merged {
                predicate.replace_column_binding(*logical_index, merged_column)?;
            }
        }
        child = SExpr::create_unary(Arc::new(Filter { predicates }.into()), Arc::new(child));
    }
    if !group_projection.is_empty() {
        child = SExpr::create_unary(
            Arc::new(
                EvalScalar {
                    items: group_projection,
                }
                .into(),
            ),
            Arc::new(child),
        );
    }

    Ok(Some(SExpr::create_unary(
        Arc::new(
            Aggregate {
                mode: AggregateMode::Initial,
                group_items: query_aggregate.group_items.clone(),
                aggregate_functions,
                from_distinct: false,
                rank_limit: None,
                grouping_sets: None,
            }
            .into(),
        ),
        Arc::new(child),
    )))
}

fn build_rollup_aggregate(
    metadata: &Metadata,
    query_aggregate: &Aggregate,
) -> Result<Option<Aggregate>> {
    let group_items = query_aggregate
        .group_items
        .iter()
        .map(|item| ScalarItem {
            index: item.index,
            scalar: column_ref(metadata, item.index),
        })
        .collect();
    let mut aggregate_functions = Vec::with_capacity(query_aggregate.aggregate_functions.len());
    for item in &query_aggregate.aggregate_functions {
        let ScalarExpr::AggregateFunction(function) = &item.scalar else {
            return Ok(None);
        };
        if function.distinct || !function.params.is_empty() || !function.sort_descs.is_empty() {
            return Ok(None);
        }
        let rollup_name = match function.func_name.as_str() {
            "sum" => "sum",
            "count" => "sum0",
            "min" => "min",
            "max" => "max",
            _ => return Ok(None),
        };
        aggregate_functions.push(ScalarItem {
            index: item.index,
            scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                span: None,
                func_name: rollup_name.to_string(),
                distinct: false,
                params: vec![],
                args: vec![column_ref(metadata, item.index)],
                return_type: function.return_type.clone(),
                sort_descs: vec![],
                display_name: function.display_name.clone(),
            }),
        });
    }

    Ok(Some(Aggregate {
        mode: AggregateMode::Initial,
        group_items,
        aggregate_functions,
        from_distinct: false,
        rank_limit: None,
        grouping_sets: None,
    }))
}

fn column_ref(metadata: &Metadata, index: crate::Symbol) -> ScalarExpr {
    ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            metadata.column(index).name(),
            index,
            Box::new(metadata.column(index).data_type()),
            Visibility::Visible,
        )
        .build(),
    })
}
