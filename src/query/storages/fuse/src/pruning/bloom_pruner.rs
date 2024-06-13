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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use log::info;
use log::warn;
use opendal::Operator;

use crate::io::BlockWriter;
use crate::io::BloomBlockFilterReader;
use crate::io::BloomIndexBuilder;

#[async_trait::async_trait]
pub trait BloomPruner {
    // returns true, if target should NOT be pruned (false positive allowed)
    async fn should_keep(
        &self,
        index_location: &Option<Location>,
        index_length: u64,
        column_stats: &StatisticsOfColumns,
        column_ids: Vec<ColumnId>,
        block_meta: &BlockMeta,
        invalid_keys: &mut Option<HashSet<String>>,
    ) -> bool;

    fn update_index_fields(
        &self,
        ignored_keys: &HashSet<String>,
    ) -> Option<Arc<dyn BloomPruner + Send + Sync>>;
}

pub struct BloomPrunerCreator {
    func_ctx: FunctionContext,

    /// indices that should be loaded from filter block
    index_fields: Vec<(TableField, String)>,

    /// the expression that would be evaluate
    filter_expression: Expr<String>,

    /// pre calculated digest for constant Scalar
    scalar_map: HashMap<Scalar, u64>,

    /// the data accessor
    dal: Operator,

    /// the schema of data being indexed
    data_schema: TableSchemaRef,

    /// bloom index builder, if set to Some(_), missing bloom index will be built during pruning
    bloom_index_builder: Option<BloomIndexBuilder>,
}

impl BloomPrunerCreator {
    pub fn create(
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        dal: Operator,
        filter_expr: Option<&Expr<String>>,
        bloom_index_cols: BloomIndexColumns,
        bloom_index_builder: Option<BloomIndexBuilder>,
    ) -> Result<Option<Arc<dyn BloomPruner + Send + Sync>>> {
        if let Some(expr) = filter_expr {
            let bloom_columns_map =
                bloom_index_cols.bloom_index_fields(schema.clone(), BloomIndex::supported_type)?;
            let bloom_column_fields = bloom_columns_map.values().cloned().collect::<Vec<_>>();
            let point_query_cols = BloomIndex::find_eq_columns(expr, bloom_column_fields)?;

            if !point_query_cols.is_empty() {
                // convert to filter column names
                let mut filter_fields = Vec::with_capacity(point_query_cols.len());
                let mut scalar_map = HashMap::<Scalar, u64>::new();
                for (field, scalar, ty, filter_key) in point_query_cols.into_iter() {
                    filter_fields.push((field, filter_key));
                    if let Entry::Vacant(e) = scalar_map.entry(scalar.clone()) {
                        let digest = BloomIndex::calculate_scalar_digest(&func_ctx, &scalar, &ty)?;
                        e.insert(digest);
                    }
                }

                let creator = BloomPrunerCreator {
                    func_ctx,
                    index_fields: filter_fields,
                    filter_expression: expr.clone(),
                    scalar_map,
                    dal,
                    data_schema: schema.clone(),
                    bloom_index_builder,
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
        column_stats: &StatisticsOfColumns,
        column_ids_of_indexed_block: Vec<ColumnId>,
        block_meta: &BlockMeta,
        invalid_keys: &mut Option<HashSet<String>>,
    ) -> Result<bool> {
        let version = index_location.1;

        // filter out columns that no longer exist in the indexed block
        let index_columns = self.index_fields.iter().try_fold(
            Vec::with_capacity(self.index_fields.len()),
            |mut acc, (field, _)| {
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

        // Perform a defensive check to ensure that the bloom index location being processed
        // matches the location specified in the block metadata.
        assert_eq!(
            Some(index_location),
            block_meta.bloom_filter_index_location.as_ref()
        );

        let maybe_filter = match (&maybe_filter, &self.bloom_index_builder) {
            (Err(_e), Some(bloom_index_builder)) => {
                // Got error while loading bloom filters, and there is Some(bloom_index_builder),
                // try rebuilding the bloom index.
                //
                // It would be better if we could directly infer from the error "_e" that the bloom
                // index being loaded does not exist. Unfortunately, in cases where the bloom index
                // does not exist, the type of error "_e" varies depending on the backend storage.

                match self
                    .try_rebuild_missing_bloom_index(
                        block_meta,
                        bloom_index_builder,
                        &index_columns,
                    )
                    .await
                {
                    Ok(Some(block_filter)) => Ok(block_filter),
                    Ok(None) => maybe_filter,
                    Err(e) => {
                        info!(
                            "failed to re-build missing index at location {:?}, {}",
                            index_location, e
                        );
                        maybe_filter
                    }
                }
            }
            _ => maybe_filter,
        };

        match maybe_filter {
            Ok(filter) => Ok(BloomIndex::from_filter_block(
                self.func_ctx.clone(),
                filter.filter_schema,
                filter.filters,
                version,
            )?
            .apply(
                self.filter_expression.clone(),
                &self.scalar_map,
                column_stats,
                self.data_schema.clone(),
                invalid_keys,
            )? != FilterEvalResult::MustFalse),
            Err(e) if e.code() == ErrorCode::DEPRECATED_INDEX_FORMAT => {
                // In case that the index is no longer supported, just return true to indicate
                // that the block being pruned should be kept. (Although the caller of this method
                // "FilterPruner::should_keep",  will ignore any exceptions returned)
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    async fn try_rebuild_missing_bloom_index(
        &self,
        block_meta: &BlockMeta,
        bloom_index_builder: &BloomIndexBuilder,
        index_columns: &[String],
    ) -> Result<Option<BlockFilter>> {
        let Some(bloom_index_location) = &block_meta.bloom_filter_index_location else {
            info!("no bloom index found in block meta, ignore");
            return Ok(None);
        };

        if self.dal.is_exist(bloom_index_location.0.as_str()).await? {
            info!("bloom index exists, ignore");
            return Ok(None);
        }

        let bloom_index_state = bloom_index_builder
            .bloom_index_state_from_block_meta(block_meta)
            .await?;

        if let Some((bloom_state, bloom_index)) = bloom_index_state {
            let column_needed: HashSet<&String> = HashSet::from_iter(index_columns.iter());
            let indexed_fields = &bloom_index.filter_schema.fields;
            let mut new_filter_schema_fields = Vec::new();
            let mut filters = Vec::new();

            for (idx, field) in indexed_fields.iter().enumerate() {
                for column_name in &column_needed {
                    if &field.name == *column_name {
                        if let Some(filter) = bloom_index.filters.get(idx) {
                            new_filter_schema_fields.push(field.clone());
                            filters.push(filter.clone())
                        }
                    }
                }
            }

            BlockWriter::write_down_bloom_index_state(
                &bloom_index_builder.table_dal,
                Some(bloom_state),
            )
            .await?;

            info!("re-created missing index {:?}", bloom_index_location);

            Ok(Some(BlockFilter {
                filter_schema: Arc::new(TableSchema::new(new_filter_schema_fields)),
                filters,
            }))
        } else {
            Ok(None)
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
        column_stats: &StatisticsOfColumns,
        column_ids: Vec<ColumnId>,
        block_meta: &BlockMeta,
        invalid_keys: &mut Option<HashSet<String>>,
    ) -> bool {
        if let Some(loc) = index_location {
            // load filter, and try pruning according to filter expression
            match self
                .apply(
                    loc,
                    index_length,
                    column_stats,
                    column_ids,
                    block_meta,
                    invalid_keys,
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    // swallow exceptions intentionally, corrupted index should not prevent execution
                    warn!("failed to apply bloom pruner, returning true. {}", e);
                    true
                }
            }
        } else {
            true
        }
    }

    fn update_index_fields(
        &self,
        ignored_keys: &HashSet<String>,
    ) -> Option<Arc<dyn BloomPruner + Send + Sync>> {
        let mut new_index_fields = Vec::with_capacity(self.index_fields.len());
        for (field, filter_key) in &self.index_fields {
            if !ignored_keys.contains(filter_key) {
                new_index_fields.push((field.clone(), filter_key.clone()));
            }
        }
        if new_index_fields.is_empty() {
            None
        } else {
            let creator = BloomPrunerCreator {
                func_ctx: self.func_ctx.clone(),
                index_fields: new_index_fields,
                filter_expression: self.filter_expression.clone(),
                scalar_map: self.scalar_map.clone(),
                dal: self.dal.clone(),
                data_schema: self.data_schema.clone(),
                bloom_index_builder: self.bloom_index_builder.clone(),
            };
            Some(Arc::new(creator))
        }
    }
}
