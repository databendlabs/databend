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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::is_internal_column;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::SEARCH_MATCHED_COL_NAME;
use databend_common_expression::SEARCH_SCORE_COL_NAME;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::ColumnSet;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::Prewhere;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::Scan;
use crate::plans::SubqueryExpr;
use crate::plans::Visitor;
use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;

pub struct RulePushDownPrewhere {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RulePushDownPrewhere {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownPrewhere,
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

    /// will throw error if the bound column ref is not in the table, such as subquery
    fn collect_columns_impl(
        table_index: IndexType,
        schema: &TableSchemaRef,
        expr: &ScalarExpr,
    ) -> Result<ColumnSet> {
        struct ColumnVisitor {
            table_index: IndexType,
            schema: TableSchemaRef,
            columns: ColumnSet,
        }
        impl<'a> Visitor<'a> for ColumnVisitor {
            fn visit_bound_column_ref(&mut self, column: &'a BoundColumnRef) -> Result<()> {
                if let Some(index) = &column.column.table_index {
                    if self.table_index == *index
                        && (column.column.visibility == Visibility::InVisible
                            || self
                                .schema
                                .index_of(column.column.column_name.as_str())
                                .is_ok())
                    {
                        if column.column.column_name == SEARCH_SCORE_COL_NAME
                            || column.column.column_name == SEARCH_MATCHED_COL_NAME
                        {
                            return Err(ErrorCode::StorageUnsupported(
                                "Prewhere don't support search functions".to_string(),
                            ));
                        }
                        if is_internal_column(&column.column.column_name) {
                            return Err(ErrorCode::StorageUnsupported(format!(
                                "Prewhere don't support internal column {}",
                                column.column.column_name
                            )));
                        }
                        self.columns.insert(column.column.index);
                        return Ok(());
                    }
                }
                Err(ErrorCode::Unimplemented("Column is not in the table"))
            }

            fn visit_subquery(&mut self, subquery: &'a SubqueryExpr) -> Result<()> {
                Err(ErrorCode::Unimplemented(format!(
                    "Prewhere don't support expr {:?}",
                    subquery
                )))
            }
        }

        let mut column_visitor = ColumnVisitor {
            table_index,
            schema: schema.clone(),
            columns: ColumnSet::new(),
        };
        // WindowFunc, SubqueryExpr and AggregateFunction will not appear in Scan
        // WindowFunc and AggFunc already check in binder:
        // Where clause can't contain aggregate or window functions
        column_visitor.visit(expr)?;
        Ok(column_visitor.columns)
    }

    // analyze if the expression can be moved to prewhere
    fn collect_columns(
        table_index: IndexType,
        schema: &TableSchemaRef,
        expr: &ScalarExpr,
    ) -> Option<ColumnSet> {
        Self::collect_columns_impl(table_index, schema, expr).ok()
    }

    pub fn prewhere_optimize(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut get: Scan = s_expr.child(0)?.plan().clone().try_into()?;
        let metadata = self.metadata.read().clone();

        let table = metadata.table(get.table_index).table();
        if !table.support_prewhere() {
            // cannot optimize
            return Ok(s_expr.clone());
        }
        let filter: Filter = s_expr.plan().clone().try_into()?;

        let mut prewhere_columns = ColumnSet::new();
        let mut prewhere_pred = Vec::new();

        // filter.predicates are already split by AND
        for pred in filter.predicates.iter() {
            match Self::collect_columns(get.table_index, &table.schema(), pred) {
                Some(columns) => {
                    prewhere_pred.push(pred.clone());
                    prewhere_columns.extend(&columns);
                }
                None => return Ok(s_expr.clone()),
            }
        }

        if !prewhere_pred.is_empty() {
            if let Some(prewhere) = get.prewhere.as_ref() {
                prewhere_pred.extend(prewhere.predicates.clone());
                prewhere_columns.extend(&prewhere.prewhere_columns);
            }

            get.prewhere = Some(Prewhere {
                output_columns: get.columns.clone(),
                prewhere_columns,
                predicates: prewhere_pred,
            });
        }
        Ok(SExpr::create_leaf(Arc::new(get.into())))
    }
}

impl Rule for RulePushDownPrewhere {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> Result<()> {
        let mut result = self.prewhere_optimize(s_expr)?;
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
