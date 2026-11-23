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
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_index::NgramLikeScalarMap;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;
use futures_util::future::try_join_all;
use log::info;
use log::warn;
use opendal::Operator;

use crate::FuseBlockPartInfo;
use crate::io::BlockWriter;
use crate::io::BloomBlockFilterReader;
use crate::io::BloomIndexRebuilder;
use crate::pruning::PruningCostController;
use crate::pruning::PruningCostKind;

#[async_trait::async_trait]
pub trait BloomPruner {
    /// Returns true if the block should NOT be pruned (false positives allowed).
    ///
    /// `rebuild_from` is the block to rebuild a missing index file from, `None` when the
    /// index must not be rebuilt.
    async fn should_keep(
        &self,
        index_files: &[BloomIndexFile<'_>],
        column_stats: &StatisticsOfColumns,
        rebuild_from: Option<&BlockReadInfo>,
    ) -> bool;
}

/// A Bloom index file of a block and the columns whose filters in it are current.
///
/// A legacy block has at most one file. A split block has one file per column group that has
/// one. A group file keeps the filters of the columns a later group rewrote, and those filters
/// describe the old values, so a file is consulted only for the columns its group serves.
pub struct BloomIndexFile<'a> {
    pub location: &'a Location,
    /// Size of this file. For a split block this is not `bloom_filter_index_size`, which
    /// sums the files of all groups.
    pub size: u64,
    pub column_ids: Vec<ColumnId>,
}

impl<'a> BloomIndexFile<'a> {
    pub fn of_block(meta: &'a BlockMeta) -> Vec<Self> {
        if !meta.is_split() {
            return meta
                .bloom_filter_index_location
                .iter()
                .map(|location| BloomIndexFile {
                    location,
                    size: meta.bloom_filter_index_size,
                    column_ids: meta.col_metas.keys().copied().collect(),
                })
                .collect();
        }
        meta.column_groups
            .iter()
            .filter_map(|group| {
                let bloom = group.bloom.as_ref()?;
                Some(BloomIndexFile {
                    location: &bloom.location,
                    size: bloom.file_size,
                    column_ids: group.leaf_column_metas.keys().copied().collect(),
                })
            })
            .collect()
    }

    /// The index files of the block of `part`, consulted only for the columns the part reads.
    pub fn of_part(part: &'a FuseBlockPartInfo) -> Vec<Self> {
        if part.column_groups.is_empty() {
            return part
                .bloom_filter_index_location
                .iter()
                .map(|location| BloomIndexFile {
                    location,
                    size: part.bloom_filter_index_size,
                    column_ids: part.columns_meta.keys().copied().collect(),
                })
                .collect();
        }
        part.column_groups
            .iter()
            .filter_map(|group| {
                let bloom = group.bloom.as_ref()?;
                Some(BloomIndexFile {
                    location: &bloom.location,
                    size: bloom.file_size,
                    column_ids: group.columns_meta.keys().copied().collect(),
                })
            })
            .collect()
    }

    fn serves(&self, field: &TableField) -> bool {
        self.column_ids.contains(&field.column_id())
    }
}

pub(crate) async fn should_prune_runtime_inlist_by_bloom_index(
    func_ctx: &FunctionContext,
    dal: &Operator,
    settings: &ReadSettings,
    cache_policy: CachePolicy,
    data_schema: &TableSchemaRef,
    expr: &Expr<String>,
    part: &FuseBlockPartInfo,
) -> Result<bool> {
    let mut index_files = BloomIndexFile::of_part(part);
    index_files.retain(|file| file.size > 0);
    if index_files.is_empty() {
        return Ok(false);
    }

    let bloom_fields = data_schema
        .fields()
        .iter()
        .filter(|field| BloomIndex::supported_type(field.data_type()))
        .cloned()
        .collect::<Vec<_>>();
    let result = BloomIndex::filter_index_field(expr, bloom_fields, vec![])?;
    if result.bloom_fields.is_empty() {
        return Ok(false);
    }

    let mut eq_scalar_map = HashMap::<Scalar, u64>::new();
    for (_, scalar, ty) in result.bloom_scalars {
        if let Entry::Vacant(entry) = eq_scalar_map.entry(scalar) {
            let digest = BloomIndex::calculate_scalar_digest(func_ctx, entry.key(), &ty)?;
            entry.insert(digest);
        }
    }

    let indexes = try_join_all(index_files.iter().map(|file| async {
        let version = file.location.1;
        let index_columns = result
            .bloom_fields
            .iter()
            .filter(|field| file.serves(field))
            .map(|field| BloomIndex::build_filter_bloom_name(version, field))
            .collect::<Result<Vec<_>>>()?;
        if index_columns.is_empty() {
            return Ok(None);
        }
        let filter = file
            .location
            .read_block_filter(
                dal.clone(),
                settings,
                &index_columns,
                file.size,
                cache_policy,
            )
            .await?;
        BloomIndex::from_filter_block(
            func_ctx.clone(),
            filter.filter_schema,
            filter.filters,
            version,
        )
        .map(Some)
    }))
    .await?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let like_scalar_map = HashMap::new();
    let empty_stats = StatisticsOfColumns::new();
    let column_stats = part.columns_stat.as_ref().unwrap_or(&empty_stats);
    Ok(BloomIndex::apply_all(
        &indexes,
        expr.clone(),
        &eq_scalar_map,
        &like_scalar_map,
        &[],
        column_stats,
        data_schema.clone(),
    )? == FilterEvalResult::MustFalse)
}

pub struct BloomPrunerCreator {
    func_ctx: FunctionContext,

    /// indices that should be loaded from filter block
    index_fields: Vec<TableField>,

    /// the expression that would be evaluate
    filter_expression: Expr<String>,

    /// pre calculated digest for constant Scalar for eq conditions
    eq_scalar_map: HashMap<Scalar, u64>,

    /// Pre-calculated digests for LIKE constants, grouped by NGRAM argument index.
    like_scalar_map: NgramLikeScalarMap,

    /// Ngram args aligned with BloomColumn using Ngram
    ngram_args: Vec<NgramArgs>,

    /// the data accessor
    dal: Operator,

    settings: ReadSettings,

    /// How the index files are read: the policy of the statement.
    cache_policy: CachePolicy,

    /// the schema of data being indexed
    data_schema: TableSchemaRef,

    /// bloom index builder, if set to Some(_), missing bloom index will be built during pruning
    bloom_index_builder: Option<BloomIndexRebuilder>,

    pruning_cost: PruningCostController,
}

impl BloomPrunerCreator {
    pub fn create(
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        dal: Operator,
        settings: ReadSettings,
        cache_policy: CachePolicy,
        filter_expr: Option<&Expr<String>>,
        bloom_index_cols: BloomIndexColumns,
        ngram_args: Vec<NgramArgs>,
        bloom_index_builder: Option<BloomIndexRebuilder>,
        pruning_cost: PruningCostController,
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
        let mut like_scalar_map = NgramLikeScalarMap::new();
        for (index, scalar) in result.ngram_scalars {
            let ngram_arg = &ngram_args[index];
            let mut digests = Vec::new();
            BloomIndex::calculate_ngram_digests(
                Value::Scalar(scalar.clone()),
                ngram_arg.gram_size(),
                ngram_arg.hash_algorithm(),
                |digest| digests.push(digest),
            );
            if !digests.is_empty() {
                like_scalar_map
                    .entry(index)
                    .or_default()
                    .entry(scalar)
                    .or_insert(digests);
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
            settings,
            cache_policy,
            data_schema: schema.clone(),
            bloom_index_builder,
            pruning_cost,
        })))
    }

    /// Returns false if the filters of `index_files` prove that the block holds no row the
    /// filter expression accepts.
    #[async_backtrace::framed]
    pub async fn apply(
        &self,
        index_files: &[BloomIndexFile<'_>],
        column_stats: &StatisticsOfColumns,
        rebuild_from: Option<&BlockReadInfo>,
    ) -> Result<bool> {
        let indexes = try_join_all(
            index_files
                .iter()
                .map(|file| self.load_index(file, rebuild_from)),
        )
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        Ok(BloomIndex::apply_all(
            &indexes,
            self.filter_expression.clone(),
            &self.eq_scalar_map,
            &self.like_scalar_map,
            &self.ngram_args,
            column_stats,
            self.data_schema.clone(),
        )? != FilterEvalResult::MustFalse)
    }

    /// Loads the filters of `file` the filter expression needs, `None` if it needs none of
    /// them or the file has a format no longer supported.
    async fn load_index(
        &self,
        file: &BloomIndexFile<'_>,
        rebuild_from: Option<&BlockReadInfo>,
    ) -> Result<Option<BloomIndex>> {
        let index_location = file.location;
        let version = index_location.1;

        // consult the file only for the columns whose filters in it are current
        let index_columns = self.index_fields.iter().try_fold(
            Vec::with_capacity(self.index_fields.len()),
            |mut acc, field| {
                if !file.serves(field) {
                    return Ok(acc);
                }
                acc.push(BloomIndex::build_filter_bloom_name(version, field)?);
                if let Some(ngram_arg) = self.ngram_args.iter().find(|arg| arg.field() == field) {
                    acc.push(BloomIndex::build_filter_ngram_name(
                        field.column_id(),
                        ngram_arg.gram_size(),
                        ngram_arg.bloom_size(),
                        ngram_arg.hash_algorithm(),
                    ));
                }
                Ok::<_, ErrorCode>(acc)
            },
        )?;
        if index_columns.is_empty() {
            return Ok(None);
        }

        // load the relevant index columns
        let maybe_filter = self
            .pruning_cost
            .measure_async(
                PruningCostKind::BlocksBloomIndexRead,
                index_location.read_block_filter(
                    self.dal.clone(),
                    &self.settings,
                    &index_columns,
                    file.size,
                    self.cache_policy,
                ),
            )
            .await;

        let maybe_filter = match (&maybe_filter, &self.bloom_index_builder, rebuild_from) {
            (Err(_e), Some(bloom_index_builder), Some(block_read_info)) => {
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
                        block_read_info,
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
            Ok(filter) => Ok(Some(BloomIndex::from_filter_block(
                self.func_ctx.clone(),
                filter.filter_schema,
                filter.filters,
                version,
            )?)),
            Err(e) if e.code() == ErrorCode::DEPRECATED_INDEX_FORMAT => {
                // An index file no longer supported prunes nothing. (The caller of this method,
                // "BloomPruner::should_keep", keeps the block on any error anyway.)
                Ok(None)
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
        index_files: &[BloomIndexFile<'_>],
        column_stats: &StatisticsOfColumns,
        rebuild_from: Option<&BlockReadInfo>,
    ) -> bool {
        if index_files.is_empty() {
            return true;
        }
        // load filter, and try pruning according to filter expression
        match self.apply(index_files, column_stats, rebuild_from).await {
            Ok(v) => v,
            Err(e) => {
                // swallow exceptions intentionally, corrupted index should not prevent execution
                warn!("failed to apply bloom pruner, returning true. {}", e);
                true
            }
        }
    }
}
