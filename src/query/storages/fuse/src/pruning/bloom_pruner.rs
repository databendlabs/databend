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
use databend_common_expression::Value;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use log::info;
use log::warn;
use opendal::Operator;

use crate::io::BlockWriter;
use crate::io::BloomBlockFilterReader;
use crate::io::BloomIndexRebuilder;

#[async_trait::async_trait]
pub trait BloomPruner {
    // returns true, if target should NOT be pruned (false positive allowed)
    async fn should_keep(
        &self,
        index_location: &Option<Location>,
        index_length: u64,
        column_stats: &StatisticsOfColumns,
        column_ids: Vec<ColumnId>,
        block_meta: &BlockReadInfo,
    ) -> bool;
}

pub struct BloomPrunerCreator {
    func_ctx: FunctionContext,

    /// indices that should be loaded from filter block
    index_fields: Vec<TableField>,

    /// the expression that would be evaluate
    filter_expression: Expr<String>,

    /// pre calculated digest for constant Scalar for eq conditions
    eq_scalar_map: HashMap<Scalar, u64>,

    /// pre calculated digest for constant Scalar for like conditions
    like_scalar_map: HashMap<Scalar, Vec<u64>>,

    /// Ngram args aligned with BloomColumn using Ngram
    ngram_args: Vec<NgramArgs>,

    /// the data accessor
    dal: Operator,

    /// the schema of data being indexed
    data_schema: TableSchemaRef,

    /// bloom index builder, if set to Some(_), missing bloom index will be built during pruning
    bloom_index_builder: Option<BloomIndexRebuilder>,
}

impl BloomPrunerCreator {
    pub fn create(
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        dal: Operator,
        filter_expr: Option<&Expr<String>>,
        bloom_index_cols: BloomIndexColumns,
        ngram_args: Vec<NgramArgs>,
        bloom_index_builder: Option<BloomIndexRebuilder>,
    ) -> Result<Option<Arc<dyn BloomPruner + Send + Sync>>> {
        let Some(expr) = filter_expr else {
            return Ok(None);
        };
        let bloom_columns_map =
            bloom_index_cols.bloom_index_fields(schema.clone(), BloomIndex::supported_type)?;
        let bloom_column_fields = bloom_columns_map.values().cloned().collect::<Vec<_>>();
        let ngram_column_fields: Vec<TableField> =
            ngram_args.iter().map(|arg| arg.field().clone()).collect();
        let mut result =
            BloomIndex::filter_index_field(expr, bloom_column_fields, ngram_column_fields)?;

        if result.bloom_fields.is_empty() && result.ngram_fields.is_empty() {
            return Ok(None);
        }

        // convert to filter column names
        let mut eq_scalar_map = HashMap::<Scalar, u64>::new();
        for (_, scalar, ty) in result.bloom_scalars.into_iter() {
            if let Entry::Vacant(e) = eq_scalar_map.entry(scalar) {
                let digest = BloomIndex::calculate_scalar_digest(&func_ctx, e.key(), &ty)?;
                e.insert(digest);
            }
        }
        let mut like_scalar_map = HashMap::<Scalar, Vec<u64>>::new();
        for (i, scalar) in result.ngram_scalars.into_iter() {
            let Some(digests) = BloomIndex::calculate_ngram_nullable_column(
                Value::Scalar(scalar.clone()),
                ngram_args[i].gram_size(),
                BloomIndex::ngram_hash,
            )
            .next() else {
                continue;
            };
            if let Entry::Vacant(e) = like_scalar_map.entry(scalar) {
                e.insert(digests);
            }
        }
        let mut index_fields = result.bloom_fields;
        index_fields.append(&mut result.ngram_fields);

        Ok(Some(Arc::new(Self {
            func_ctx,
            index_fields,
            filter_expression: expr.clone(),
            eq_scalar_map,
            like_scalar_map,
            ngram_args,
            dal,
            data_schema: schema.clone(),
            bloom_index_builder,
        })))
    }

    // Check a location file is hit or not by bloom filter.
    #[async_backtrace::framed]
    pub async fn apply(
        &self,
        index_location: &Location,
        index_length: u64,
        column_stats: &StatisticsOfColumns,
        column_ids_of_indexed_block: Vec<ColumnId>,
        block_meta: &BlockReadInfo,
    ) -> Result<bool> {
        let version = index_location.1;

        // filter out columns that no longer exist in the indexed block
        let index_columns = self.index_fields.iter().try_fold(
            Vec::with_capacity(self.index_fields.len()),
            |mut acc, field| {
                if column_ids_of_indexed_block.contains(&field.column_id()) {
                    acc.push(BloomIndex::build_filter_bloom_name(version, field)?);
                }
                if let Some(ngram_arg) = self.ngram_args.iter().find(|arg| arg.field() == field) {
                    acc.push(BloomIndex::build_filter_ngram_name(
                        field.column_id(),
                        ngram_arg.gram_size(),
                        ngram_arg.bloom_size(),
                    ));
                }
                Ok::<_, ErrorCode>(acc)
            },
        )?;

        // load the relevant index columns
        let maybe_filter = index_location
            .read_block_filter(self.dal.clone(), &index_columns, index_length)
            .await;

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
                        index_location,
                        bloom_index_builder,
                        &index_columns,
                        block_meta,
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
                &self.eq_scalar_map,
                &self.like_scalar_map,
                &self.ngram_args,
                column_stats,
                self.data_schema.clone(),
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
        bloom_index_location: &Location,
        bloom_index_builder: &BloomIndexRebuilder,
        index_columns: &[String],
        block_read_info: &BlockReadInfo,
    ) -> Result<Option<BlockFilter>> {
        if self.dal.exists(bloom_index_location.0.as_str()).await? {
            info!("bloom index exists, ignore");
            return Ok(None);
        }

        let bloom_index_state = bloom_index_builder
            .bloom_index_state_from_block_meta(bloom_index_location, block_read_info)
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
        block_meta: &BlockReadInfo,
    ) -> bool {
        if let Some(loc) = index_location {
            // load filter, and try pruning according to filter expression
            match self
                .apply(loc, index_length, column_stats, column_ids, block_meta)
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
}
