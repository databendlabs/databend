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

use databend_common_expression::is_stream_column;
use databend_common_expression::types::DataType;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::Statistics;
use crate::BaseTableColumn;
use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::ColumnSet;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;

pub struct RuleSubqueryNotInToIn {
    id: RuleID,
    matchers: Vec<Matcher>,
    pub(crate) metadata: MetadataRef,
}

impl RuleSubqueryNotInToIn {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::SubqueryNotInToIn,
            matchers: vec![Matcher::MatchFn {
                predicate: Box::new(|op| {
                    if let RelOperator::Filter(filter) = op {
                        return filter.predicates.len() == 1;
                    }
                    false
                }),
                children: vec![Matcher::MatchFn {
                    predicate: Box::new(|op| {
                        if let RelOperator::Join(join) = op {
                            // only subquery rightmark
                            return join.join_type == JoinType::RightMark;
                        }
                        false
                    }),
                    children: vec![Matcher::Leaf, Matcher::Leaf],
                }],
            }],
            metadata,
        }
    }
}

impl Rule for RuleSubqueryNotInToIn {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut join: Join = s_expr.children[0].plan().clone().try_into()?;

        let (ScalarExpr::FunctionCall(call), Some(mark_index)) =
            (&filter.predicates[0], join.marker_index)
        else {
            return Ok(());
        };
        if call.func_name != "not"
            || call.arguments.len() != 1
            || call.arguments[0]
                .used_columns()
                .iter()
                .any(|index| *index != mark_index)
        {
            return Ok(());
        }
        // subquery only one eq condition(subquery child_expr = subquery output column)
        debug_assert!(join.equi_conditions.len() == 1);

        let condition = &join.equi_conditions[0];
        let ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) = &condition.left else {
            return Ok(());
        };
        let (Some(table_index), Some(column_pos)) = (
            column.source_table_index.or(column.table_index),
            column.column_position,
        ) else {
            return Ok(());
        };
        let (in_expr_column, in_expr_scan) = {
            let mut guard = self.metadata.write();
            let table_entry = guard.table(table_index).clone();
            let new_table_name = format!("subquery_in_expr_table_{}", table_entry.name());
            let (new_table_index, _) = guard.add_table(
                table_entry.catalog().to_string(),
                table_entry.database().to_string(),
                table_entry.table(),
                Some(new_table_name.clone()),
                table_entry.is_source_of_view(),
                table_entry.is_source_of_index(),
                table_entry.is_source_of_stage(),
                None,
            );
            let ColumnEntry::BaseTableColumn(BaseTableColumn {
                column_name,
                column_index,
                path_indices,
                data_type,
                column_position,
                virtual_expr,
                ..
            }) = &guard.columns_by_table_index(new_table_index)[column_pos - 1]
            // start from 1
            else {
                return Ok(());
            };
            let scan_id = guard.next_scan_id();
            let mut base_column_scan_id = HashMap::with_capacity(1);
            base_column_scan_id.insert(*column_index, scan_id);
            guard.add_base_column_scan_id(base_column_scan_id);

            let column_binding = ColumnBindingBuilder::new(
                column_name.clone(),
                *column_index,
                Box::new(DataType::from(data_type)),
                if path_indices.is_some() || is_stream_column(column_name) {
                    Visibility::InVisible
                } else {
                    Visibility::Visible
                },
            )
            .table_name(Some(new_table_name))
            .database_name(Some(table_entry.database().to_string()))
            .table_index(Some(new_table_index))
            .source_table_index(Some(table_index))
            .column_position(*column_position)
            .virtual_expr(virtual_expr.clone())
            .build();
            let scan = SExpr::create_leaf(Arc::new(
                Scan {
                    table_index: new_table_index,
                    columns: ColumnSet::from_iter([*column_index]),
                    statistics: Arc::new(Statistics::default()),
                    scan_id,
                    ..Default::default()
                }
                .into(),
            ));
            (column_binding, scan)
        };
        let subquery_body = s_expr.children[0].children[1].clone();
        let in_expr_column_expr = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: in_expr_column.clone(),
        });
        let subquery_output = condition.right.clone();

        join.equi_conditions[0].right = in_expr_column_expr.clone();
        state.add_result(SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: vec![call.arguments[0].clone()],
                }
                .into(),
            ),
            Arc::new(SExpr::create_binary(
                Arc::new(join.into()),
                s_expr.children[0].children[0].clone(),
                Arc::new(SExpr::create_unary(
                    Arc::new(RelOperator::Filter(Filter {
                        predicates: vec![ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            params: vec![],
                            arguments: vec![ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "is_not_null".to_string(),
                                params: vec![],
                                arguments: vec![subquery_output.clone()],
                            })],
                            func_name: "not".to_string(),
                        })],
                    })),
                    Arc::new(SExpr::create_binary(
                        Arc::new(
                            Join {
                                equi_conditions: JoinEquiCondition::new_conditions(
                                    vec![in_expr_column_expr],
                                    vec![subquery_output],
                                    vec![],
                                ),
                                non_equi_conditions: vec![],
                                join_type: JoinType::Left,
                                marker_index: None,
                                from_correlated_subquery: false,
                                need_hold_hash_table: false,
                                is_lateral: false,
                                single_to_inner: None,
                                build_side_cache_info: None,
                            }
                            .into(),
                        ),
                        Arc::new(in_expr_scan),
                        subquery_body.clone(),
                    )),
                )),
            )),
        ));
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
