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
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use log::info;

use crate::ColumnEntry;
use crate::IndexType;
use crate::Metadata;
use crate::ScalarExpr;
use crate::Symbol;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::SExpr;
use crate::plans::AggIndexInfo;
use crate::plans::BoundColumnRef;
use crate::plans::RelOperator;
use super::super::view_rewrite::QueryInfo;
use super::super::view_rewrite::format_scalar;
use super::super::view_rewrite::ViewInfo;
use super::super::view_rewrite::ViewMatcher;

pub fn try_rewrite(
    table_index: IndexType,
    table_name: &str,
    metadata: &Metadata,
    s_expr: &SExpr,
    index_plans: &[(u64, String, SExpr)],
) -> Result<Option<SExpr>> {
    if index_plans.is_empty() {
        return Ok(None);
    }

    let query_info = QueryInfo::new(
        table_index,
        table_name,
        metadata.columns_by_table_index(table_index),
        s_expr,
    )?;
    let view_matcher = ViewMatcher::new(query_info);

    for (index_id, sql, view_s_expr) in index_plans.iter() {
        let backend = AggIndexView::new(
            table_index,
            table_name,
            metadata.columns_by_table_index(table_index),
            view_s_expr,
        )?;
        if let Some(matched) = view_matcher.try_match(&backend.view_info)? {
            let result = push_down_index_scan(s_expr, AggIndexInfo {
                index_id: *index_id,
                selection: matched.selection,
                predicates: matched.predicates,
                schema: TableSchemaRefExt::create(backend.index_fields),
                is_agg: matched.is_aggregate,
                num_agg_funcs: matched.num_aggregate_functions,
            })?;
            info!("Use aggregating index: {sql}");
            return Ok(Some(result));
        }
    }

    Ok(None)
}

// Record information of aggregating index plan.
struct AggIndexView {
    view_info: ViewInfo,
    index_fields: Vec<TableField>,
}

impl AggIndexView {
    fn new<'a>(
        table_index: IndexType,
        table_name: &str,
        base_columns: impl IntoIterator<Item = &'a ColumnEntry>,
        s_expr: &SExpr,
    ) -> Result<Self> {
        let base_columns = base_columns.into_iter().collect::<Vec<_>>();
        let query_info = QueryInfo::new(table_index, table_name, base_columns.iter().copied(), s_expr)?;

        // collect the output columns of aggregating index,
        // query can use those columns to compute expressions.
        let mut index_fields = Vec::with_capacity(query_info.output_cols.len());
        let mut index_output_cols = HashMap::with_capacity(query_info.output_cols.len());
        let factory = AggregateFunctionFactory::instance();
        for (index, item) in query_info.output_cols.iter().enumerate() {
            let display_name = format_scalar(&item.scalar, &query_info.column_map);

            let aggr_scalar_item = query_info.aggregate.as_ref().and_then(|aggregate| {
                aggregate
                    .aggregate_functions
                    .iter()
                    .find(|agg_func| agg_func.index == item.index)
            });

            let (data_type, is_agg) = match aggr_scalar_item {
                Some(item) => {
                    let func = match &item.scalar {
                        ScalarExpr::AggregateFunction(func) => func,
                        _ => unreachable!(),
                    };
                    let func = factory.get(
                        &func.func_name,
                        func.params.clone(),
                        func.args
                            .iter()
                            .map(|arg| arg.data_type())
                            .collect::<Result<_>>()?,
                        func.sort_descs
                            .iter()
                            .map(|desc| desc.try_into())
                            .collect::<Result<_>>()?,
                    )?;
                    (func.serialize_data_type(), true)
                }
                None => (item.scalar.data_type().unwrap(), false),
            };

            let name = index.to_string();
            let table_ty = infer_schema_type(&data_type)?;
            let index_field = TableField::new(&name, table_ty);
            index_fields.push(index_field);

            let index_scalar = to_index_scalar(index, &data_type);
            index_output_cols.insert(display_name, (index_scalar, is_agg));
        }

        let view_info = ViewInfo::new(
            table_index,
            table_name,
            base_columns.iter().copied(),
            s_expr,
            index_output_cols,
        )?;
        Ok(Self {
            view_info,
            index_fields,
        })
    }
}

fn to_index_scalar(index: FieldIndex, data_type: &DataType) -> ScalarExpr {
    let col = BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            format!("index_col_{index}"),
            Symbol::from_field_index(index),
            Box::new(data_type.clone()),
            Visibility::Visible,
        )
        .build(),
    };
    ScalarExpr::BoundColumnRef(col)
}

fn push_down_index_scan(s_expr: &SExpr, agg_info: AggIndexInfo) -> Result<SExpr> {
    Ok(match s_expr.plan() {
        RelOperator::Scan(scan) => {
            let mut new_scan = scan.clone();
            new_scan.agg_index = Some(agg_info);
            s_expr.replace_plan(Arc::new(new_scan.into()))
        }
        _ => {
            let child = push_down_index_scan(s_expr.child(0)?, agg_info)?;
            s_expr.replace_children(vec![Arc::new(child)])
        }
    })
}
