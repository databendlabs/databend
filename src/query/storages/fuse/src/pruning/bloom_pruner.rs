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
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::cast_scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexResult;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;
use databend_storages_common_table_meta::meta::supported_stat_type;
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

    /// columns used by bloom filter expressions
    bloom_fields: Vec<TableField>,
    /// columns used by ngram filter expressions
    ngram_fields: Vec<TableField>,

    /// the expression that would be evaluate
    filter_expression: Expr<String>,

    /// pre calculated digest for constant Scalar for eq conditions
    eq_scalar_map: HashMap<Scalar, u64>,

    /// pre calculated digest for constant Scalar for like conditions
    like_scalar_map: HashMap<Scalar, Vec<u64>>,

    /// scalars bound to bloom fields (field, scalar, data_type)
    bloom_scalars: Vec<(TableField, Scalar, DataType)>,
    /// scalars bound to ngram fields (field, scalar)
    ngram_scalars: Vec<(TableField, Scalar)>,

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
        let result = BloomIndex::filter_index_field(
            expr,
            bloom_column_fields.clone(),
            ngram_column_fields.clone(),
        )?;

        if result.bloom_fields.is_empty() && result.ngram_fields.is_empty() {
            return Ok(None);
        }

        let BloomIndexResult {
            bloom_fields,
            ngram_fields,
            bloom_scalars,
            ngram_scalars,
        } = result;

        // convert to filter column names
        let mut eq_scalar_map = HashMap::<Scalar, u64>::new();
        for (_, scalar, ty) in bloom_scalars.iter() {
            if let Entry::Vacant(e) = eq_scalar_map.entry(scalar.clone()) {
                let digest = BloomIndex::calculate_scalar_digest(&func_ctx, scalar, ty)?;
                e.insert(digest);
            }
        }
        let mut like_scalar_map = HashMap::<Scalar, Vec<u64>>::new();
        for (i, scalar) in ngram_scalars.iter() {
            let Some(digests) = BloomIndex::calculate_ngram_nullable_column(
                Value::Scalar(scalar.clone()),
                ngram_args[*i].gram_size(),
                BloomIndex::ngram_hash,
            )
            .next() else {
                continue;
            };
            if let Entry::Vacant(e) = like_scalar_map.entry(scalar.clone()) {
                e.insert(digests);
            }
        }
        let bloom_scalars = bloom_scalars
            .iter()
            .filter_map(|(idx, scalar, ty)| {
                bloom_column_fields
                    .get(*idx)
                    .map(|field| (field.clone(), scalar.clone(), ty.clone()))
            })
            .collect::<Vec<_>>();
        let ngram_scalars = ngram_scalars
            .iter()
            .filter_map(|(idx, scalar)| {
                ngram_column_fields
                    .get(*idx)
                    .map(|field| (field.clone(), scalar.clone()))
            })
            .collect::<Vec<_>>();

        let mut index_fields = bloom_fields.clone();
        index_fields.extend(ngram_fields.clone());

        Ok(Some(Arc::new(Self {
            func_ctx,
            index_fields,
            bloom_fields,
            ngram_fields,
            filter_expression: expr.clone(),
            eq_scalar_map,
            like_scalar_map,
            bloom_scalars,
            ngram_scalars,
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

        let mut stats_type_by_id = HashMap::<ColumnId, DataType>::new();
        for field in self.bloom_fields.iter().chain(self.ngram_fields.iter()) {
            if !column_ids_of_indexed_block.contains(&field.column_id()) {
                continue;
            }
            let stats_column_id = map_value_leaf_id(field).unwrap_or_else(|| field.column_id());
            let digest_type = bloom_digest_type(field);
            let Some(stat) = column_stats.get(&stats_column_id) else {
                if !supported_stat_type(&bloom_source_type(field)) {
                    stats_type_by_id.insert(field.column_id(), digest_type);
                    continue;
                }
                return Ok(true);
            };
            let stat_type = if !stat.max().is_null() {
                stat.max().as_ref().infer_data_type()
            } else if !stat.min().is_null() {
                stat.min().as_ref().infer_data_type()
            } else if !supported_stat_type(&bloom_source_type(field)) {
                digest_type
            } else {
                return Ok(true);
            };
            stats_type_by_id.insert(field.column_id(), stat_type);
        }

        let mut use_fast_path = true;
        for field in self.bloom_fields.iter().chain(self.ngram_fields.iter()) {
            if !column_ids_of_indexed_block.contains(&field.column_id()) {
                continue;
            }
            let Some(stat_type) = stats_type_by_id.get(&field.column_id()) else {
                use_fast_path = false;
                break;
            };
            let expected_type = bloom_digest_type(field);
            if stat_type.remove_nullable() != expected_type.remove_nullable() {
                use_fast_path = false;
                break;
            }
        }

        let mut local_eq_scalar_map = HashMap::<Scalar, u64>::new();
        let mut local_like_scalar_map = HashMap::<Scalar, Vec<u64>>::new();
        if !use_fast_path {
            for (field, scalar, _ty) in self.bloom_scalars.iter() {
                if !column_ids_of_indexed_block.contains(&field.column_id()) {
                    continue;
                }
                let Some(stat_type) = stats_type_by_id.get(&field.column_id()) else {
                    return Ok(true);
                };
                let casted =
                    match cast_scalar(Span::None, scalar.clone(), stat_type, &BUILTIN_FUNCTIONS) {
                        Ok(s) => s,
                        Err(_) => return Ok(true),
                    };
                if casted.is_null() {
                    return Ok(true);
                }
                if let Entry::Vacant(e) = local_eq_scalar_map.entry(scalar.clone()) {
                    let digest =
                        BloomIndex::calculate_scalar_digest(&self.func_ctx, &casted, stat_type)?;
                    e.insert(digest);
                }
            }

            for (field, scalar) in self.ngram_scalars.iter() {
                if !column_ids_of_indexed_block.contains(&field.column_id()) {
                    continue;
                }
                let Some(stat_type) = stats_type_by_id.get(&field.column_id()) else {
                    return Ok(true);
                };
                if stat_type.remove_nullable() != DataType::String {
                    return Ok(true);
                }
                let casted =
                    match cast_scalar(Span::None, scalar.clone(), stat_type, &BUILTIN_FUNCTIONS) {
                        Ok(s) => s,
                        Err(_) => return Ok(true),
                    };
                if casted.is_null() {
                    return Ok(true);
                }
                let Some(ngram_arg) = self.ngram_args.iter().find(|arg| arg.field() == field)
                else {
                    return Ok(true);
                };
                let Some(digests) = BloomIndex::calculate_ngram_nullable_column(
                    Value::Scalar(casted),
                    ngram_arg.gram_size(),
                    BloomIndex::ngram_hash,
                )
                .next() else {
                    continue;
                };
                if let Entry::Vacant(e) = local_like_scalar_map.entry(scalar.clone()) {
                    e.insert(digests);
                }
            }
        }

        let eq_scalar_map = if use_fast_path {
            &self.eq_scalar_map
        } else {
            &local_eq_scalar_map
        };
        let like_scalar_map = if use_fast_path {
            &self.like_scalar_map
        } else {
            &local_like_scalar_map
        };

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
                eq_scalar_map,
                like_scalar_map,
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

fn map_value_leaf_id(field: &TableField) -> Option<ColumnId> {
    let data_type = field.data_type().remove_nullable();
    let TableDataType::Map(inner) = data_type else {
        return None;
    };
    let TableDataType::Tuple { fields_type, .. } = inner.as_ref() else {
        return None;
    };
    if fields_type.len() < 2 {
        return None;
    }
    let key_leaf_count = fields_type[0].num_leaf_columns();
    let leaf_ids = field.leaf_column_ids();
    leaf_ids.get(key_leaf_count).copied()
}

fn map_value_data_type(field: &TableField) -> Option<DataType> {
    let data_type = field.data_type().remove_nullable();
    let TableDataType::Map(inner) = data_type else {
        return None;
    };
    let TableDataType::Tuple { fields_type, .. } = inner.as_ref() else {
        return None;
    };
    if fields_type.len() < 2 {
        return None;
    }
    Some(DataType::from(&fields_type[1]))
}

fn bloom_source_type(field: &TableField) -> DataType {
    if let Some(map_value) = map_value_data_type(field) {
        return map_value;
    }
    DataType::from(field.data_type())
}

fn bloom_digest_type(field: &TableField) -> DataType {
    let source_type = bloom_source_type(field);
    if source_type.remove_nullable() == DataType::Variant {
        return DataType::String;
    }
    source_type
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
