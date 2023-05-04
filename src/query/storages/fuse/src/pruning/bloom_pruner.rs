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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use opendal::Operator;
use storages_common_index::BloomIndex;
use storages_common_index::FilterEvalResult;
use storages_common_table_meta::meta::Location;

use crate::io::BloomBlockFilterReader;

#[async_trait::async_trait]
pub trait BloomPruner {
    // returns true, if target should NOT be pruned (false positive allowed)
    async fn should_keep(
        &self,
        index_location: &Option<Location>,
        index_length: u64,
        column_ids: Vec<ColumnId>,
    ) -> bool;
}

pub struct BloomPrunerCreator {
    func_ctx: FunctionContext,

    /// indices that should be loaded from filter block
    index_fields: Vec<TableField>,

    /// the expression that would be evaluate
    filter_expression: Expr<String>,

    /// pre calculated digest for constant Scalar
    scalar_map: HashMap<Scalar, u64>,

    /// the data accessor
    dal: Operator,

    /// the schema of data being indexed
    data_schema: TableSchemaRef,
}

impl BloomPrunerCreator {
    pub fn create(
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        dal: Operator,
        filter_expr: Option<&Expr<String>>,
    ) -> Result<Option<Arc<dyn BloomPruner + Send + Sync>>> {
        if let Some(expr) = filter_expr {
            let point_query_cols = BloomIndex::find_eq_columns(expr)?;

            if !point_query_cols.is_empty() {
                // convert to filter column names
                let mut filter_fields = Vec::with_capacity(point_query_cols.len());
                let mut scalar_map = HashMap::<Scalar, u64>::new();
                for (col_name, scalar, ty) in point_query_cols.iter() {
                    if let Ok(field) = schema.field_with_name(col_name) {
                        filter_fields.push(field.clone());
                        if !scalar_map.contains_key(scalar) {
                            let digest =
                                BloomIndex::calculate_scalar_digest(&func_ctx, scalar, ty)?;
                            scalar_map.insert(scalar.clone(), digest);
                        }
                    }
                }

                let creator = BloomPrunerCreator {
                    func_ctx,
                    index_fields: filter_fields,
                    filter_expression: expr.clone(),
                    scalar_map,
                    dal,
                    data_schema: schema.clone(),
                };
                return Ok(Some(Arc::new(creator)));
            }
        }
        Ok(None)
    }

    // Check a location file is hit or not by bloom filter.
    #[async_backtrace::framed]
    pub async fn apply(
        &self,
        index_location: &Location,
        index_length: u64,
        column_ids_of_indexed_block: Vec<ColumnId>,
    ) -> Result<bool> {
        let version = index_location.1;

        // filter out columns that no longer exist in the indexed block
        let index_columns = self.index_fields.iter().try_fold(
            Vec::with_capacity(self.index_fields.len()),
            |mut acc, field| {
                if column_ids_of_indexed_block.contains(&field.column_id()) {
                    acc.push(BloomIndex::build_filter_column_name(version, field)?);
                }
                Ok::<_, ErrorCode>(acc)
            },
        )?;
        // load the relevant index columns
        let maybe_filter = index_location
            .read_block_filter(self.dal.clone(), &index_columns, index_length)
            .await;

        match maybe_filter {
            Ok(filter) => Ok(BloomIndex::from_filter_block(
                self.func_ctx.clone(),
                self.data_schema.clone(),
                filter.filter_schema,
                filter.filters,
                version,
            )?
            .apply(self.filter_expression.clone(), &self.scalar_map)?
                != FilterEvalResult::MustFalse),
            Err(e) if e.code() == ErrorCode::DEPRECATED_INDEX_FORMAT => {
                // In case that the index is no longer supported, just return true to indicate
                // that the block being pruned should be kept. (Although the caller of this method
                // "FilterPruner::should_keep",  will ignore any exceptions returned)
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl BloomPruner for BloomPrunerCreator {
    #[async_backtrace::framed]
    async fn should_keep(
        &self,
        index_location: &Option<Location>,
        index_length: u64,
        column_ids: Vec<ColumnId>,
    ) -> bool {
        if let Some(loc) = index_location {
            // load filter, and try pruning according to filter expression
            match self.apply(loc, index_length, column_ids).await {
                Ok(v) => v,
                Err(e) => {
                    // swallow exceptions intentionally, corrupted index should not prevent execution
                    tracing::warn!("failed to apply bloom pruner, returning true. {}", e);
                    true
                }
            }
        } else {
            true
        }
    }
}
