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
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_fuse_meta::meta::Location;
use common_legacy_planners::Expression;
use common_legacy_planners::ExpressionVisitor;
use common_legacy_planners::Recursion;
use common_storage::StorageParams;
use common_storages_index::BloomFilterIndexer;
use opendal::Operator;

use crate::io::BlockBloomFilterIndexReader;

#[async_trait::async_trait]
pub trait BloomFilterPruner {
    // returns ture, if target should NOT be pruned (false positive allowed)
    async fn should_keep(&self, index_location: &Option<Location>, index_length: u64) -> bool;
}

/// dummy pruner that prunes nothing
pub(crate) struct NonPruner;

#[async_trait::async_trait]
impl BloomFilterPruner for NonPruner {
    async fn should_keep(&self, _: &Option<Location>, _index_length: u64) -> bool {
        true
    }
}

struct BloomFilterIndexPruner {
    ctx: Arc<dyn TableContext>,
    // columns that should be loaded from bloom filter block
    index_columns: Vec<String>,
    // the expression that would be evaluate
    filter_expression: Expression,
    // the data accessor
    dal: Operator,
    // the schema of data being indexed
    data_schema: DataSchemaRef,
    storage_params: Option<StorageParams>,
}

impl BloomFilterIndexPruner {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        index_columns: Vec<String>,
        filter_expression: Expression,
        dal: Operator,
        data_schema: DataSchemaRef,
        storage_params: Option<StorageParams>,
    ) -> Self {
        Self {
            ctx,
            index_columns,
            filter_expression,
            dal,
            data_schema,
            storage_params,
        }
    }
}

use self::util::*;
#[async_trait::async_trait]
impl BloomFilterPruner for BloomFilterIndexPruner {
    async fn should_keep(&self, index_location: &Option<Location>, index_length: u64) -> bool {
        if let Some(loc) = index_location {
            // load bloom filter index, and try pruning according to filter expression
            match filter_block_by_bloom_index(
                self.ctx.clone(),
                self.dal.clone(),
                &self.data_schema,
                &self.filter_expression,
                &self.index_columns,
                loc,
                index_length,
                self.storage_params.clone(),
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    // swallow exceptions intentionally, corrupted index should not prevent execution
                    tracing::warn!("failed to apply bloom filter, returning ture. {}", e);
                    true
                }
            }
        } else {
            true
        }
    }
}

/// try to build the pruner.
/// if `filter_expr` is none, or is not applicable, e.g. have no point queries
/// a [NonPruner] will be return, which prunes nothing.
/// otherwise, a [BloomFilterIndexer] backed pruner will be return
pub fn new_bloom_filter_pruner(
    ctx: &Arc<dyn TableContext>,
    filter_exprs: Option<&[Expression]>,
    schema: &DataSchemaRef,
    dal: Operator,
    storage_params: Option<StorageParams>,
) -> Result<Arc<dyn BloomFilterPruner + Send + Sync>> {
    if let Some(exprs) = filter_exprs {
        if exprs.is_empty() {
            return Ok(Arc::new(NonPruner));
        }
        // check if there were applicable filter conditions
        let expr = exprs
            .iter()
            .fold(None, |acc: Option<Expression>, item| match acc {
                Some(acc) => Some(acc.and(item.clone())),
                None => Some(item.clone()),
            })
            .unwrap();

        let point_query_cols = columns_names_of_eq_expressions(&expr)?;
        if !point_query_cols.is_empty() {
            // convert to bloom filter block's column names
            let filter_block_cols = point_query_cols
                .into_iter()
                .map(|n| BloomFilterIndexer::to_bloom_column_name(&n))
                .collect();
            return Ok(Arc::new(BloomFilterIndexPruner::new(
                ctx.clone(),
                filter_block_cols,
                expr,
                dal,
                schema.clone(),
                storage_params,
            )));
        } else {
            tracing::debug!("no point filters found, using NonPruner");
        }
    }
    Ok(Arc::new(NonPruner))
}

mod util {
    use super::*;
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn filter_block_by_bloom_index(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        schema: &DataSchemaRef,
        filter_expr: &Expression,
        bloom_index_col_names: &[String],
        index_location: &Location,
        index_length: u64,
        storage_params: Option<StorageParams>,
    ) -> Result<bool> {
        // load the relevant index columns
        let bloom_filter_index = index_location
            .read_bloom_filter_index(
                ctx.clone(),
                dal,
                bloom_index_col_names,
                index_length,
                storage_params,
            )
            .await?;

        // figure it out
        BloomFilterIndexer::from_bloom_block(schema.clone(), bloom_filter_index.into_data(), ctx)?
            .maybe_true(filter_expr)
    }

    struct PointQueryVisitor {
        // names of columns which used by point query kept here
        columns: HashSet<String>,
    }

    impl ExpressionVisitor for PointQueryVisitor {
        fn pre_visit(mut self, expr: &Expression) -> Result<Recursion<Self>> {
            // TODO
            // 1. only binary op "=" is considered, which is NOT enough
            // 2. should combine this logic with BloomFilterIndexer
            match expr {
                Expression::BinaryExpression { left, op, right } if op.as_str() == "=" => {
                    match (left.as_ref(), right.as_ref()) {
                        (Expression::Column(column), Expression::Literal { .. })
                        | (Expression::Literal { .. }, Expression::Column(column)) => {
                            self.columns.insert(column.clone());
                            Ok(Recursion::Stop(self))
                        }
                        _ => Ok(Recursion::Continue(self)),
                    }
                }
                _ => Ok(Recursion::Continue(self)),
            }
        }
    }

    pub fn columns_names_of_eq_expressions(filter_expr: &Expression) -> Result<Vec<String>> {
        let visitor = PointQueryVisitor {
            columns: HashSet::new(),
        };

        filter_expr
            .accept(visitor)
            .map(|r| r.columns.into_iter().collect())
    }
}
