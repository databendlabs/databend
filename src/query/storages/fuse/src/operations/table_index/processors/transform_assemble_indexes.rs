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

//! Merge per-index contributions into one replacement block metadata.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_pipeline_transforms::AsyncTransform;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::Versioned;
use log::info;
use opendal::Operator;
use parquet::file::metadata::KeyValue;

use crate::io::BlockWriter;
use crate::io::BloomIndexState;
use crate::io::SpatialIndexBuilder;
use crate::io::SpatialIndexResult;
use crate::io::TableMetaLocationGenerator;
use crate::io::read::bloom::BloomColumnFilterReader;
use crate::io::read::bloom::block_filter_reader::load_bloom_filter_by_columns;
use crate::io::read::load_spatial_index_files;
use crate::io::read::load_vector_index_meta;
use crate::operations::MutationLogEntry;
use crate::operations::MutationLogs;
use crate::operations::table_index::meta::BuiltIndexes;
use crate::operations::table_index::meta::BuiltSpatialIndex;
use crate::operations::table_index::meta::ExistingIndexes;
use crate::operations::table_index::meta::IndexArtifact;
use crate::operations::table_index::meta::WrittenVectorIndex;

#[derive(Clone)]
pub struct AssembleIndexes {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
    locations: TableMetaLocationGenerator,
    active_vector_versions: BTreeMap<String, String>,
}
#[async_trait::async_trait]
impl AsyncTransform for AssembleIndexes {
    const NAME: &'static str = "AssembleRefreshIndexes";
    async fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let BuiltIndexes { plan, artifacts } = block
            .get_owned_meta()
            .and_then(BuiltIndexes::downcast_from)
            .ok_or_else(|| ErrorCode::Internal("Missing built refresh indexes"))?;
        let mut updated = (*plan.meta).clone();
        let mut blooms = Vec::new();
        let mut spatial = Vec::new();
        let mut vectors = Vec::new();
        let mut inverted = Vec::new();
        for artifact in artifacts {
            match artifact {
                IndexArtifact::Inverted(index) => inverted.push(index),
                IndexArtifact::Vector(index) => vectors.push(index),
                IndexArtifact::PendingVector(_) => {
                    return Err(ErrorCode::Internal("Unwritten vector refresh artifact"));
                }
                IndexArtifact::Bloom(index) => blooms.push(index),
                IndexArtifact::Spatial(index) => spatial.push(index),
            }
        }
        if !inverted.is_empty() {
            let mut metas = updated.inverted_index_metas.take().unwrap_or_default();
            metas.retain(|old| !inverted.iter().any(|new| new.index_name == old.index_name));
            // Preserve the existing accounting: retained bundle sizes plus the
            // newly written bundles and their siblings.
            let retained_size: u64 = metas.iter().map(|meta| meta.size).sum();
            let total = retained_size + inverted.iter().map(|index| index.total_size).sum::<u64>();
            metas.extend(
                inverted
                    .into_iter()
                    .map(|index| index.to_block_index_meta()),
            );
            metas.sort_unstable_by(|a, b| a.index_name.cmp(&b.index_name));
            updated.inverted_index_size = Some(total);
            updated.inverted_index_metas = Some(metas);
        }
        if !vectors.is_empty() {
            self.merge_vectors(&mut updated, vectors).await?;
        }
        if !blooms.is_empty() {
            self.merge_blooms(&mut updated, blooms, &plan.existing)
                .await?;
        }
        if !spatial.is_empty() {
            self.merge_spatial(&mut updated, spatial, &plan.existing)
                .await?;
        }
        Ok(MutationLogs {
            entries: vec![MutationLogEntry::ReplacedBlock {
                index: plan.index.clone(),
                block_meta: Arc::new(ExtendedBlockMeta {
                    block_meta: updated,
                    draft_virtual_block_meta: None,
                    column_hlls: plan.hlls.clone().map(BlockHLLState::Serialized),
                    column_top_n: plan.top_n.clone(),
                }),
            }],
            ..Default::default()
        }
        .into())
    }
}

impl AssembleIndexes {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        locations: TableMetaLocationGenerator,
        active_vector_versions: BTreeMap<String, String>,
    ) -> Self {
        Self {
            ctx,
            operator,
            locations,
            active_vector_versions,
        }
    }

    async fn merge_vectors(
        &self,
        block: &mut BlockMeta,
        results: Vec<WrittenVectorIndex>,
    ) -> Result<()> {
        let mut metas = block.vector_index_metas.take().unwrap_or_default();
        let old_named_size: u64 = metas.iter().map(|meta| meta.size).sum();
        let legacy_size = if block.vector_index_location.is_some() {
            block
                .vector_index_size
                .unwrap_or(0)
                .checked_sub(old_named_size)
                .ok_or_else(|| ErrorCode::StorageOther("Invalid vector index size accounting"))?
        } else {
            0
        };
        metas.retain(|meta| {
            self.active_vector_versions.get(&meta.index_name) == Some(&meta.index_version)
        });
        let mut stats = block.vector_stats.take().unwrap_or_default();
        for result in results {
            for meta in result.metas {
                metas.retain(|old| old.index_name != meta.index_name);
                metas.push(meta);
            }
            if let Some(new_stats) = result.statistics {
                stats.extend(new_stats);
            }
        }
        metas.sort_unstable_by(|a, b| a.index_name.cmp(&b.index_name));
        let keep_legacy = match &block.vector_index_location {
            Some(location) => {
                legacy_serves_other_index(
                    &self.operator,
                    &location.0,
                    &self.active_vector_versions,
                    &metas,
                    self.ctx.cache_policy(),
                )
                .await?
            }
            None => false,
        };
        if !keep_legacy {
            block.vector_index_location = None;
        }
        let total = metas.iter().map(|meta| meta.size).sum::<u64>()
            + if keep_legacy { legacy_size } else { 0 };
        block.vector_index_size = (total != 0).then_some(total);
        block.vector_index_metas = Some(metas);
        block.vector_stats = (!stats.is_empty()).then_some(stats);
        Ok(())
    }

    async fn merge_blooms(
        &self,
        block: &mut BlockMeta,
        results: Vec<BloomIndex>,
        existing: &ExistingIndexes,
    ) -> Result<()> {
        // Preserve Ngram refresh's fixed path and read/modify/write semantics.
        // Only combine contributions here: ordinary Bloom filters are reused,
        // never rebuilt or migrated as part of refresh.
        let path = TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(
            &block.location.0,
        );
        let mut fields = Vec::new();
        let mut filters = Vec::new();
        let mut replaced_prefixes = HashSet::new();
        for result in &results {
            for field in result.filter_schema.fields() {
                let prefix = field
                    .name()
                    .split_once(')')
                    .map(|(prefix, _)| format!("{prefix})"))
                    .ok_or_else(|| ErrorCode::Internal("Invalid Ngram filter name"))?;
                replaced_prefixes.insert(prefix);
            }
        }
        let mut old_ngram_names = Vec::new();
        if let Some(meta) = &existing.bloom {
            let names = meta
                .columns
                .iter()
                .filter_map(|(name, _)| {
                    if replaced_prefixes
                        .iter()
                        .any(|prefix| name.starts_with(prefix))
                    {
                        old_ngram_names.push(name.clone());
                        None
                    } else {
                        Some(name.clone())
                    }
                })
                .collect::<Vec<_>>();
            if !names.is_empty() {
                let old = load_bloom_filter_by_columns(
                    self.operator.clone(),
                    &ReadSettings::from_ctx(&self.ctx)?,
                    &names,
                    &path,
                    block.bloom_filter_index_size,
                    self.ctx.cache_policy(),
                )
                .await?;
                fields.extend(old.filter_schema.fields().iter().cloned());
                filters.extend(old.filters);
            }
        }
        for result in results {
            for (field, filter) in result
                .filter_schema
                .fields()
                .iter()
                .zip(result.filters.iter())
            {
                fields.push(field.clone());
                filters.push(filter.clone());
            }
        }
        let bloom = BloomIndex::from_filter_block(
            self.ctx.get_function_context()?,
            Arc::new(TableSchema::new(fields)),
            filters,
            BlockFilter::VERSION,
        )?;
        let state =
            BloomIndexState::from_bloom_index(&bloom, (path.clone(), BlockFilter::VERSION))?;
        block.bloom_filter_index_size = state.size();
        block.ngram_filter_index_size = state.ngram_size();
        let old_filter_keys = old_ngram_names
            .iter()
            .map(|name| BloomColumnFilterReader::cache_key(&path, name))
            .collect();
        BlockWriter::rewrite_bloom_index_state(&self.operator, state, old_filter_keys).await?;
        Ok(())
    }

    async fn merge_spatial(
        &self,
        block: &mut BlockMeta,
        results: Vec<BuiltSpatialIndex>,
        existing: &ExistingIndexes,
    ) -> Result<()> {
        let start = Instant::now();
        info!(
            "Start assemble spatial indexes for block: {}",
            block.location.0
        );
        let mut columns = BTreeMap::new();
        let mut metadata = BTreeMap::new();
        let mut stats = block.spatial_stats.take().unwrap_or_default();
        let mut replaced = HashSet::new();
        let mut replaced_indexes = HashSet::new();
        for BuiltSpatialIndex {
            columns: result,
            stats: new_stats,
            ids,
            name,
        } in results
        {
            replaced_indexes.insert(name);
            replaced.extend(ids.iter().map(ToString::to_string));
            stats.retain(|id, _| !ids.contains(id));
            if let Some(new_stats) = new_stats {
                stats.extend(new_stats);
            }
            for (field, column) in result.index_fields.into_iter().zip(result.index_columns) {
                columns.insert(field.name().clone(), column);
            }
            for entry in result.metadata {
                if let Some(version) = entry.value {
                    metadata.insert(entry.key, version);
                }
            }
        }
        if let Some(old) = &existing.spatial {
            let location = block
                .spatial_index_location
                .as_ref()
                .ok_or_else(|| ErrorCode::Internal("Missing spatial index location"))?;
            let names = old
                .columns
                .iter()
                .map(|(name, _)| name)
                .filter(|name| !replaced.contains(name.strip_suffix("_srid").unwrap_or(name)))
                .cloned()
                .collect::<Vec<_>>();
            let kept = load_spatial_index_files(
                self.operator.clone(),
                &ReadSettings::from_ctx(&self.ctx)?,
                &names,
                &location.0,
                self.ctx.cache_policy(),
            )
            .await?;
            for (name, column) in names.into_iter().zip(kept) {
                columns.insert(name, BlockEntry::Column(column));
            }
            for (name, version) in &old.metadata {
                if !replaced_indexes.contains(name) {
                    metadata
                        .entry(name.clone())
                        .or_insert_with(|| version.clone());
                }
            }
        }
        let mut result = SpatialIndexResult::default();
        for (name, column) in columns {
            result.index_fields.push(TableField::new(
                &name,
                if name.ends_with("_srid") {
                    TableDataType::Number(NumberDataType::Int32)
                } else {
                    TableDataType::Binary
                },
            ));
            result.index_columns.push(column);
        }
        result.metadata = metadata
            .into_iter()
            .map(|(key, value)| KeyValue {
                key,
                value: Some(value),
            })
            .collect();
        let state = if result.index_fields.is_empty() {
            None
        } else {
            Some(SpatialIndexBuilder::serialize_spatial_index(
                result,
                &self.locations.block_spatial_index_location(),
            )?)
        };
        block.spatial_index_size = state.as_ref().map(|state| state.size);
        block.spatial_index_location = state.as_ref().map(|state| state.location.clone());
        block.spatial_stats = (!stats.is_empty()).then_some(stats);
        info!(
            "Finish assemble spatial indexes for block: {}, cost={} ms",
            block.location.0,
            start.elapsed().as_millis()
        );
        BlockWriter::write_down_spatial_index_state(&self.operator, state).await?;
        Ok(())
    }
}

/// Keep a legacy combined file only while it serves another active index which
/// has not migrated to a current bundle. Fully migrated blocks need no I/O.
async fn legacy_serves_other_index(
    operator: &Operator,
    path: &str,
    active_versions: &BTreeMap<String, String>,
    metas: &[BlockIndexMeta],
    policy: CachePolicy,
) -> Result<bool> {
    let unmigrated = {
        let mut indexes = Vec::new();
        for (name, version) in active_versions {
            if !metas
                .iter()
                .any(|meta| &meta.index_name == name && &meta.index_version == version)
            {
                indexes.push((name.clone(), version.clone()));
            }
        }
        indexes
    };
    if unmigrated.is_empty() {
        return Ok(false);
    }
    let meta = load_vector_index_meta(operator.clone(), path, policy).await?;
    let VectorIndexMeta::Legacy(meta) = meta.as_ref() else {
        return Ok(false);
    };
    Ok(unmigrated
        .into_iter()
        .any(|(name, version)| meta.metadata.get(&name) == Some(&version)))
}
