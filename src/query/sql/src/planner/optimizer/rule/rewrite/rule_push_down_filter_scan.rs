// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::Scan;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::TableEntry;

pub struct RulePushDownFilterScan {
    id: RuleID,
    patterns: Vec<SExpr>,
    metadata: MetadataRef,
}

impl RulePushDownFilterScan {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownFilterScan,
            // Filter
            //  \
            //   LogicalGet
            patterns: vec![SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Scan,
                    }
                    .into(),
                ),
            )],
            metadata,
        }
    }

    // Using the columns of the source table to replace the columns in the view,
    // this allows us to perform push-down filtering operations at the storage layer.
    fn replace_view_column(
        predicate: &ScalarExpr,
        table_entries: &[TableEntry],
        column_entries: &[ColumnEntry],
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
                        let column_binding = ColumnBinding {
                            database_name: Some(table_entry.database().to_string()),
                            table_name: Some(table_entry.name().to_string()),
                            table_index: Some(table_entry.index()),
                            column_name: base_column.column_name.clone(),
                            index: base_column.column_index,
                            data_type: column.column.data_type.clone(),
                            visibility: column.column.visibility.clone(),
                        };
                        let bound_column_ref = BoundColumnRef {
                            span: column.span,
                            column: column_binding,
                        };
                        return Ok(ScalarExpr::BoundColumnRef(bound_column_ref));
                    }
                }
                Ok(predicate.clone())
            }
            ScalarExpr::AndExpr(scalar) => {
                let left = Self::replace_view_column(&scalar.left, table_entries, column_entries)?;
                let right =
                    Self::replace_view_column(&scalar.right, table_entries, column_entries)?;
                Ok(ScalarExpr::AndExpr(AndExpr {
                    left: Box::new(left),
                    right: Box::new(right),
                }))
            }
            ScalarExpr::OrExpr(scalar) => {
                let left = Self::replace_view_column(&scalar.left, table_entries, column_entries)?;
                let right =
                    Self::replace_view_column(&scalar.right, table_entries, column_entries)?;
                Ok(ScalarExpr::OrExpr(OrExpr {
                    left: Box::new(left),
                    right: Box::new(right),
                }))
            }
            ScalarExpr::NotExpr(scalar) => {
                let argument =
                    Self::replace_view_column(&scalar.argument, table_entries, column_entries)?;
                Ok(ScalarExpr::NotExpr(NotExpr {
                    argument: Box::new(argument),
                }))
            }
            ScalarExpr::ComparisonExpr(scalar) => {
                let left = Self::replace_view_column(&scalar.left, table_entries, column_entries)?;
                let right =
                    Self::replace_view_column(&scalar.right, table_entries, column_entries)?;
                Ok(ScalarExpr::ComparisonExpr(ComparisonExpr {
                    op: scalar.op.clone(),
                    left: Box::new(left),
                    right: Box::new(right),
                }))
            }
            ScalarExpr::WindowFunction(window) => {
                let func = match &window.func {
                    WindowFuncType::Aggregate(agg) => {
                        let args = agg
                            .args
                            .iter()
                            .map(|arg| {
                                Self::replace_view_column(arg, table_entries, column_entries)
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
                    func => func.clone(),
                };

                let partition_by = window
                    .partition_by
                    .iter()
                    .map(|arg| Self::replace_view_column(arg, table_entries, column_entries))
                    .collect::<Result<Vec<ScalarExpr>>>()?;

                let order_by = window
                    .order_by
                    .iter()
                    .map(|item| {
                        let replaced_scalar =
                            Self::replace_view_column(&item.expr, table_entries, column_entries)?;
                        Ok(WindowOrderBy {
                            expr: replaced_scalar,
                            asc: item.asc,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<WindowOrderBy>>>()?;

                Ok(ScalarExpr::WindowFunction(WindowFunc {
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
                    .map(|arg| Self::replace_view_column(arg, table_entries, column_entries))
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
            ScalarExpr::FunctionCall(func) => {
                let arguments = func
                    .arguments
                    .iter()
                    .map(|arg| Self::replace_view_column(arg, table_entries, column_entries))
                    .collect::<Result<Vec<ScalarExpr>>>()?;

                Ok(ScalarExpr::FunctionCall(FunctionCall {
                    span: func.span,
                    params: func.params.clone(),
                    arguments,
                    func_name: func.func_name.clone(),
                }))
            }
            ScalarExpr::CastExpr(cast) => {
                let arg = Self::replace_view_column(&cast.argument, table_entries, column_entries)?;
                Ok(ScalarExpr::CastExpr(CastExpr {
                    span: cast.span,
                    is_try: cast.is_try,
                    argument: Box::new(arg),
                    target_type: cast.target_type.clone(),
                }))
            }
            _ => Ok(predicate.clone()),
        }
    }

    fn find_push_down_predicates(&self, predicates: &[ScalarExpr]) -> Result<Vec<ScalarExpr>> {
        let metadata = self.metadata.read();
        let column_entries = metadata.columns();
        let table_entries = metadata.tables();
        let is_source_of_view = table_entries.iter().any(|t| t.is_source_of_view());

        let mut filtered_predicates = vec![];
        for predicate in predicates {
            let used_columns = predicate.used_columns();
            let mut contain_derived_column = false;
            for column_entry in column_entries {
                match column_entry {
                    ColumnEntry::BaseTableColumn(_) => {}
                    ColumnEntry::InternalColumn(_) => {}
                    ColumnEntry::DerivedColumn(column) => {
                        // Don't push down predicate that contains derived column
                        // Because storage can't know such columns.
                        if used_columns.contains(&column.column_index) {
                            contain_derived_column = true;
                            break;
                        }
                    }
                    ColumnEntry::VirtualColumn(_) => {}
                }
            }
            if !contain_derived_column {
                if is_source_of_view {
                    let new_predicate =
                        Self::replace_view_column(predicate, table_entries, column_entries)?;
                    filtered_predicates.push(new_predicate);
                } else {
                    filtered_predicates.push(predicate.clone());
                }
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
        let mut get: Scan = s_expr.child(0)?.plan().clone().try_into()?;

        let add_filters = self.find_push_down_predicates(&filter.predicates)?;

        match get.push_down_predicates.as_mut() {
            Some(vs) => vs.extend(add_filters),
            None => get.push_down_predicates = Some(add_filters),
        }

        let mut result = SExpr::create_unary(filter.into(), SExpr::create_leaf(get.into()));
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
