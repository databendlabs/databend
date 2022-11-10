//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::plan::Expression;
use common_catalog::table_context::TableContext;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_sql::executor::ExpressionOp;
use common_storages_index::BlockFilter;
use common_storages_table_meta::meta::Location;
use opendal::Operator;

use crate::io::BlockFilterReader;

#[async_trait::async_trait]
pub trait Pruner {
    // returns ture, if target should NOT be pruned (false positive allowed)
    async fn should_keep(&self, index_location: &Option<Location>, index_length: u64) -> bool;
}

struct FilterPruner {
    ctx: Arc<dyn TableContext>,

    /// indices that should be loaded from filter block
    index_columns: Vec<String>,

    /// the expression that would be evaluate
    filter_expression: Expression,

    /// the data accessor
    dal: Operator,

    /// the schema of data being indexed
    data_schema: DataSchemaRef,
}

impl FilterPruner {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        index_columns: Vec<String>,
        filter_expression: Expression,
        dal: Operator,
        data_schema: DataSchemaRef,
    ) -> Self {
        Self {
            ctx,
            index_columns,
            filter_expression,
            dal,
            data_schema,
        }
    }
}

use self::util::*;
#[async_trait::async_trait]
impl Pruner for FilterPruner {
    async fn should_keep(&self, index_location: &Option<Location>, index_length: u64) -> bool {
        if let Some(loc) = index_location {
            // load filter, and try pruning according to filter expression
            match should_keep_by_filter(
                self.ctx.clone(),
                self.dal.clone(),
                &self.data_schema,
                &self.filter_expression,
                &self.index_columns,
                loc,
                index_length,
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    // swallow exceptions intentionally, corrupted index should not prevent execution
                    tracing::warn!("failed to apply filter, returning ture. {}", e);
                    true
                }
            }
        } else {
            true
        }
    }
}

/// Try to build a pruner.
///
/// if `filter_expr` is empty, or is not applicable, e.g. have no point queries
/// a [NonPruner] will be return, which prunes nothing.
/// otherwise, a [Filter] backed pruner will be return
pub fn new_filter_pruner(
    ctx: &Arc<dyn TableContext>,
    filter_exprs: Option<&[Expression]>,
    schema: &DataSchemaRef,
    dal: Operator,
) -> Result<Option<Arc<dyn Pruner + Send + Sync>>> {
    if let Some(exprs) = filter_exprs {
        if exprs.is_empty() {
            return Ok(None);
        }
        // check if there were applicable filter conditions
        let expr = exprs
            .iter()
            .fold(None, |acc: Option<Expression>, item| match acc {
                Some(acc) => Some(acc.and(item).unwrap()),
                None => Some(item.clone()),
            })
            .unwrap();

        let point_query_cols = columns_of_eq_expressions(&expr)?;
        if !point_query_cols.is_empty() {
            // convert to filter column names
            let filter_block_cols = point_query_cols
                .iter()
                .map(|n| BlockFilter::build_filter_column_name(n))
                .collect();

            return Ok(Some(Arc::new(FilterPruner::new(
                ctx.clone(),
                filter_block_cols,
                expr,
                dal,
                schema.clone(),
            ))));
        } else {
            tracing::debug!("no point filters found, using NonPruner");
        }
    }
    Ok(None)
}

mod util {
    use common_catalog::plan::ExpressionVisitor;
    use common_catalog::plan::Recursion;
    use common_exception::ErrorCode;

    use super::*;
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn should_keep_by_filter(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        schema: &DataSchemaRef,
        filter_expr: &Expression,
        filter_col_names: &[String],
        index_location: &Location,
        index_length: u64,
    ) -> Result<bool> {
        // load the relevant index columns
        let maybe_filter = index_location
            .read_filter(ctx.clone(), dal, filter_col_names, index_length)
            .await;

        match maybe_filter {
            // figure it out
            Ok(filter) => BlockFilter::from_filter_block(schema.clone(), filter.into_data())?
                .maybe_true(filter_expr),
            Err(e) if e.code() == ErrorCode::DEPRECATED_INDEX_FORMAT => {
                // In case that the index is no longer supported, just return ture to indicate
                // that the block being pruned should be kept. (Although the caller of this method
                // "FilterPruner::should_keep",  will ignore any exceptions returned)
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    struct PointQueryVisitor {
        // indices of columns which used by point query kept here
        columns: HashSet<String>,
    }

    impl ExpressionVisitor for PointQueryVisitor {
        fn pre_visit(mut self, expr: &Expression) -> Result<Recursion<Self>> {
            // 1. only binary op "=" is considered, which is NOT enough
            // 2. should combine this logic with Filter
            match expr {
                Expression::Function { name, args, .. }
                    if name.as_str() == "=" && args.len() == 2 =>
                {
                    match (&args[0], &args[1]) {
                        (Expression::IndexedVariable { name, .. }, Expression::Constant { .. })
                        | (Expression::Constant { .. }, Expression::IndexedVariable { name, .. }) =>
                        {
                            self.columns.insert(name.clone());
                            Ok(Recursion::Stop(self))
                        }
                        _ => Ok(Recursion::Continue(self)),
                    }
                }
                _ => Ok(Recursion::Continue(self)),
            }
        }
    }

    pub fn columns_of_eq_expressions(filter_expr: &Expression) -> Result<Vec<String>> {
        let visitor = PointQueryVisitor {
            columns: HashSet::new(),
        };

        filter_expr
            .accept(visitor)
            .map(|r| r.columns.into_iter().collect())
    }
}
