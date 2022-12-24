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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::Expr;
use common_expression::TableSchemaRef;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_storages_index::ChunkFilter;
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
    filter_expression: Expr<String>,

    /// the data accessor
    dal: Operator,

    /// the schema of data being indexed
    data_schema: TableSchemaRef,
}

impl FilterPruner {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        index_columns: Vec<String>,
        filter_expression: Expr<String>,
        dal: Operator,
        data_schema: TableSchemaRef,
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
    filter_exprs: Option<&[Expr<String>]>,
    schema: &TableSchemaRef,
    dal: Operator,
) -> Result<Option<Arc<dyn Pruner + Send + Sync>>> {
    if let Some(exprs) = filter_exprs {
        if exprs.is_empty() {
            return Ok(None);
        }

        // Check if there were applicable filter conditions.
        let expr: Expr<String> = exprs
            .iter()
            .cloned()
            .reduce(|lhs, rhs| {
                check_function(None, "and", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS).unwrap()
            })
            .unwrap();

        let point_query_cols = ChunkFilter::find_eq_columns(&expr)?;
        if !point_query_cols.is_empty() {
            // convert to filter column names
            let filter_block_cols = point_query_cols
                .iter()
                .map(|n| ChunkFilter::build_filter_column_name(n))
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
    use common_exception::ErrorCode;
    use common_storages_index::FilterEvalResult;

    use super::*;
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn should_keep_by_filter(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        schema: &TableSchemaRef,
        filter_expr: &Expr<String>,
        filter_col_names: &[String],
        index_location: &Location,
        index_length: u64,
    ) -> Result<bool> {
        // load the relevant index columns
        let maybe_filter = index_location
            .read_filter(ctx.clone(), dal, filter_col_names, index_length)
            .await;

        match maybe_filter {
            Ok(filter) => Ok(ChunkFilter::from_filter_chunk(
                ctx.try_get_function_context()?,
                schema.clone(),
                schema.clone(),
                filter.into_data(),
            )?
            .eval(filter_expr.clone())?
                != FilterEvalResult::MustFalse),
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
}
