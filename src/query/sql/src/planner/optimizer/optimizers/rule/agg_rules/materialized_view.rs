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
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::Filter;

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
    let matcher = ViewMatcher::new(query_info);

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

        info!(
            "Use materialized view {}: {}",
            candidate.mv_table_id, candidate.logical_sql
        );
        return Ok(Some((replacement, candidate.mv_table_id)));
    }

    Ok(None)
}
