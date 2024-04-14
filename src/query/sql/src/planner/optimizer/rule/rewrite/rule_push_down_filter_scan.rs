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

use crate::binder::ColumnBindingBuilder;
use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::LagLeadFunction;
use crate::plans::LambdaFunc;
use crate::plans::NthValueFunction;
use crate::plans::RelOp;
use crate::plans::Scan;
use crate::plans::UDFCall;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;
use crate::ColumnEntry;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::TableEntry;

pub struct RulePushDownFilterScan {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RulePushDownFilterScan {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownFilterScan,
            // Filter
            //  \
            //   Scan
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Scan,
                    children: vec![],
                }],
            }],
            metadata,
        }
    }

    // Replace columns in a predicate.
    // If replace_view is true, we will use the columns of the source table to replace the columns in
    // the view, this allows us to perform push-down filtering operations at the storage layer.
    // If replace_view is false, we will replace column alias name with original column name.
    fn replace_predicate_column(
        predicate: &ScalarExpr,
        table_entries: &[TableEntry],
        column_entries: &[&ColumnEntry],
        replace_view: bool,
    ) -> Result<ScalarExpr> {
        match predicate {
            ScalarExpr::BoundColumnRef(column) => {
                if let Some(base_column) =
                    column_entries
                        .iter()
                        .find_map(|column_entry| match column_entry {
                            ColumnEntry::BaseTableColumn(base_column)
                                if base_column.column_index == column.column.index =>
                            {
                                Some(base_column)
                            }
                            _ => None,
                        })
                {
                    if let Some(table_entry) = table_entries
                        .iter()
                        .find(|table_entry| table_entry.index() == base_column.table_index)
                    {
                        let mut column_binding_builder = ColumnBindingBuilder::new(
                            base_column.column_name.clone(),
                            base_column.column_index,
                            column.column.data_type.clone(),
                            column.column.visibility.clone(),
                        )
                        .table_name(Some(table_entry.name().to_string()))
                        .database_name(Some(table_entry.database().to_string()))
                        .table_index(Some(table_entry.index()));

                        if replace_view {
                            column_binding_builder = column_binding_builder
                                .virtual_computed_expr(column.column.virtual_computed_expr.clone());
                        }

                        let bound_column_ref = BoundColumnRef {
                            span: column.span,
                            column: column_binding_builder.build(),
                        };
                        return Ok(ScalarExpr::BoundColumnRef(bound_column_ref));
                    }
                }
                Ok(predicate.clone())
            }
            ScalarExpr::WindowFunction(window) => {
                let func = match &window.func {
                    WindowFuncType::Aggregate(agg) => {
                        let args = agg
                            .args
                            .iter()
                            .map(|arg| {
                                Self::replace_predicate_column(
                                    arg,
                                    table_entries,
                                    column_entries,
                                    replace_view,
                                )
                            })
                            .collect::<Result<Vec<ScalarExpr>>>()?;

                        WindowFuncType::Aggregate(AggregateFunction {
                            func_name: agg.func_name.clone(),
                            distinct: agg.distinct,
                            params: agg.params.clone(),
                            args,
                            return_type: agg.return_type.clone(),
                            display_name: agg.display_name.clone(),
                        })
                    }
                    WindowFuncType::LagLead(ll) => {
                        let new_arg = Self::replace_predicate_column(
                            &ll.arg,
                            table_entries,
                            column_entries,
                            replace_view,
                        )?;
                        let new_default = match ll.default.clone().map(|d| {
                            Self::replace_predicate_column(
                                &d,
                                table_entries,
                                column_entries,
                                replace_view,
                            )
                        }) {
                            None => None,
                            Some(d) => Some(Box::new(d?)),
                        };
                        WindowFuncType::LagLead(LagLeadFunction {
                            is_lag: ll.is_lag,
                            arg: Box::new(new_arg),
                            offset: ll.offset,
                            default: new_default,
                            return_type: ll.return_type.clone(),
                        })
                    }
                    WindowFuncType::NthValue(func) => {
                        let new_arg = Self::replace_predicate_column(
                            &func.arg,
                            table_entries,
                            column_entries,
                            replace_view,
                        )?;
                        WindowFuncType::NthValue(NthValueFunction {
                            n: func.n,
                            arg: Box::new(new_arg),
                            return_type: func.return_type.clone(),
                        })
                    }
                    func => func.clone(),
                };

                let partition_by = window
                    .partition_by
                    .iter()
                    .map(|arg| {
                        Self::replace_predicate_column(
                            arg,
                            table_entries,
                            column_entries,
                            replace_view,
                        )
                    })
                    .collect::<Result<Vec<ScalarExpr>>>()?;

                let order_by = window
                    .order_by
                    .iter()
                    .map(|item| {
                        let replaced_scalar = Self::replace_predicate_column(
                            &item.expr,
                            table_entries,
                            column_entries,
                            replace_view,
                        )?;
                        Ok(WindowOrderBy {
                            expr: replaced_scalar,
                            asc: item.asc,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<WindowOrderBy>>>()?;

                Ok(ScalarExpr::WindowFunction(WindowFunc {
                    span: window.span,
                    display_name: window.display_name.clone(),
                    func,
                    partition_by,
                    order_by,
                    frame: window.frame.clone(),
                }))
            }
            ScalarExpr::AggregateFunction(agg_func) => {
                let args = agg_func
                    .args
                    .iter()
                    .map(|arg| {
                        Self::replace_predicate_column(
                            arg,
                            table_entries,
                            column_entries,
                            replace_view,
                        )
                    })
                    .collect::<Result<Vec<ScalarExpr>>>()?;

                Ok(ScalarExpr::AggregateFunction(AggregateFunction {
                    func_name: agg_func.func_name.clone(),
                    distinct: agg_func.distinct,
                    params: agg_func.params.clone(),
                    args,
                    return_type: agg_func.return_type.clone(),
                    display_name: agg_func.display_name.clone(),
                }))
            }
            ScalarExpr::LambdaFunction(lambda_func) => {
                let args = lambda_func
                    .args
                    .iter()
                    .map(|arg| {
                        Self::replace_predicate_column(
                            arg,
                            table_entries,
                            column_entries,
                            replace_view,
                        )
                    })
                    .collect::<Result<Vec<ScalarExpr>>>()?;

                Ok(ScalarExpr::LambdaFunction(LambdaFunc {
                    span: lambda_func.span,
                    func_name: lambda_func.func_name.clone(),
                    args,
                    lambda_expr: lambda_func.lambda_expr.clone(),
                    lambda_display: lambda_func.lambda_display.clone(),
                    return_type: lambda_func.return_type.clone(),
                }))
            }
            ScalarExpr::FunctionCall(func) => {
                let arguments = func
                    .arguments
                    .iter()
                    .map(|arg| {
                        Self::replace_predicate_column(
                            arg,
                            table_entries,
                            column_entries,
                            replace_view,
                        )
                    })
                    .collect::<Result<Vec<ScalarExpr>>>()?;

                Ok(ScalarExpr::FunctionCall(FunctionCall {
                    span: func.span,
                    params: func.params.clone(),
                    arguments,
                    func_name: func.func_name.clone(),
                }))
            }
            ScalarExpr::CastExpr(cast) => {
                let arg = Self::replace_predicate_column(
                    &cast.argument,
                    table_entries,
                    column_entries,
                    replace_view,
                )?;
                Ok(ScalarExpr::CastExpr(CastExpr {
                    span: cast.span,
                    is_try: cast.is_try,
                    argument: Box::new(arg),
                    target_type: cast.target_type.clone(),
                }))
            }
            ScalarExpr::UDFCall(udf) => {
                let arguments = udf
                    .arguments
                    .iter()
                    .map(|arg| {
                        Self::replace_predicate_column(
                            arg,
                            table_entries,
                            column_entries,
                            replace_view,
                        )
                    })
                    .collect::<Result<Vec<ScalarExpr>>>()?;

                Ok(ScalarExpr::UDFCall(UDFCall {
                    span: udf.span,
                    name: udf.name.clone(),
                    func_name: udf.func_name.clone(),
                    display_name: udf.display_name.clone(),
                    udf_type: udf.udf_type.clone(),
                    arg_types: udf.arg_types.clone(),
                    return_type: udf.return_type.clone(),
                    arguments,
                }))
            }

            _ => Ok(predicate.clone()),
        }
    }

    fn find_push_down_predicates(
        &self,
        predicates: &[ScalarExpr],
        scan: &Scan,
    ) -> Result<Vec<ScalarExpr>> {
        let metadata = self.metadata.read();
        let column_entries = scan
            .columns
            .iter()
            .map(|index| metadata.column(*index))
            .collect::<Vec<_>>();
        let table_entries = metadata.tables();
        let is_source_of_view = table_entries.iter().any(|t| t.is_source_of_view());

        let mut filtered_predicates = vec![];
        for predicate in predicates {
            let used_columns = predicate.used_columns();
            let mut contain_derived_column = false;
            for column_entry in column_entries.iter() {
                if let ColumnEntry::DerivedColumn(column) = column_entry {
                    // Don't push down predicate that contains derived column
                    // Because storage can't know such columns.
                    if used_columns.contains(&column.column_index) {
                        contain_derived_column = true;
                        break;
                    }
                }
            }
            if !contain_derived_column {
                let predicate = Self::replace_predicate_column(
                    predicate,
                    table_entries,
                    &column_entries,
                    is_source_of_view,
                )?;
                filtered_predicates.push(predicate);
            }
        }

        Ok(filtered_predicates)
    }
}

impl Rule for RulePushDownFilterScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut scan: Scan = s_expr.child(0)?.plan().clone().try_into()?;

        let add_filters = self.find_push_down_predicates(&filter.predicates, &scan)?;

        match scan.push_down_predicates.as_mut() {
            Some(vs) => vs.extend(add_filters),
            None => scan.push_down_predicates = Some(add_filters),
        }

        let mut result = SExpr::create_unary(
            Arc::new(filter.into()),
            Arc::new(SExpr::create_leaf(Arc::new(scan.into()))),
        );
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
