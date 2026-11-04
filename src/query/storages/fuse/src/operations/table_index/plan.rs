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

//! Select refresh work and a shared projection from one snapshot.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::try_join_all;
use futures::stream::iter;
use opendal::ErrorKind;
use opendal::Operator;

use crate::FuseTable;
use crate::io::FuseCaches;
use crate::io::SegmentInfoReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::read::bloom::block_filter_reader::load_index_meta;
use crate::io::read::load_spatial_index_meta;
use crate::io::read::read_segment_stats;
use crate::operations::BlockMetaIndex;
use crate::operations::table_index::get_refresh_index;
use crate::operations::table_index::inverted_index::RefreshInvertedIndex;
use crate::operations::table_index::meta::ExistingIndexes;
use crate::operations::table_index::meta::IndexSpec;
use crate::operations::table_index::meta::RefreshBlock;
use crate::operations::table_index::meta::RefreshPlan;

const REFRESH_CHECK_CONCURRENCY: usize = 32;

pub async fn collect_refresh_plan(
    table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    snapshot: &TableSnapshot,
    index_names: &[String],
    segment_locs: Option<Vec<Location>>,
) -> Result<RefreshPlan> {
    let schema = table.schema();
    let operator = table.get_operator();
    let mut specs = Vec::with_capacity(index_names.len());
    for name in index_names.iter().collect::<BTreeSet<_>>() {
        let index = get_refresh_index(table, name)?;
        let fields = index
            .column_ids
            .iter()
            .map(|id| {
                schema
                    .fields()
                    .iter()
                    .position(|field| field.column_id() == *id)
                    .ok_or_else(|| {
                        ErrorCode::RefreshIndexError(format!(
                            "Index {name} references missing column {id}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        specs.push(IndexSpec {
            index: index.clone(),
            schema: Arc::new(schema.project(&fields)),
            fields,
            inverted: None,
        });
    }
    let check_bloom = specs
        .iter()
        .any(|spec| spec.index.index_type == TableIndexType::Ngram);
    let check_spatial = specs
        .iter()
        .any(|spec| spec.index.index_type == TableIndexType::Spatial);
    let targets = segment_locs.map(|locations| locations.into_iter().collect::<HashSet<_>>());
    let segment_reader = SegmentInfoReader::new(operator.clone(), schema.clone());
    let cache = FuseCaches::instance().segments.read_only();
    let mut readers = BTreeMap::new();
    let mut blocks = VecDeque::new();
    // Locate targets only once. No other segment metadata or block data is read.
    for (segment_idx, location) in snapshot.segments.iter().enumerate() {
        if targets
            .as_ref()
            .is_some_and(|targets| !targets.contains(location))
        {
            continue;
        }
        let segment = cache.get(&segment_reader, location.clone()).await?;
        let metas = segment.block_metas()?;
        let stats = match segment.summary.additional_stats_loc() {
            Some(location) => {
                let mut stats =
                    Arc::unwrap_or_clone(read_segment_stats(operator.clone(), location).await?);
                stats.align_to_blocks(metas.len())?;
                Some(stats)
            }
            None => None,
        };
        let checks = iter(metas.into_iter().enumerate())
            .map(|(block_idx, meta)| {
                let specs = &specs;
                let operator = &operator;
                let stats = &stats;
                async move {
                    let existing =
                        ExistingIndexes::read(operator, &meta, check_bloom, check_spatial).await?;
                    let missing = try_join_all(specs.iter().map(|spec| async {
                        Ok::<_, ErrorCode>(
                            needs_refresh(operator, &meta, &spec.index, &spec.schema, &existing)
                                .await?
                                .then(|| spec.index.name.clone()),
                        )
                    }))
                    .await?
                    .into_iter()
                    .flatten()
                    .collect::<HashSet<_>>();
                    Ok::<_, ErrorCode>((
                        block_idx,
                        meta,
                        missing,
                        existing,
                        stats
                            .as_ref()
                            .and_then(|stats| stats.block_hlls.get(block_idx))
                            .cloned(),
                        stats
                            .as_ref()
                            .and_then(|stats| stats.block_top_ns.get(block_idx))
                            .cloned(),
                    ))
                }
            })
            .buffered(REFRESH_CHECK_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        for (block_idx, meta, missing, existing, hlls, top_n) in checks {
            if missing.is_empty() {
                continue;
            }
            let projection = specs
                .iter()
                .filter(|spec| missing.contains(&spec.index.name))
                .flat_map(|spec| spec.fields.iter().copied())
                .collect::<BTreeSet<_>>();
            let projection = projection.into_iter().collect::<Vec<_>>();
            let reader = match readers.get(&projection) {
                Some(reader) => Arc::clone(reader),
                None => {
                    let reader = table.create_block_reader(
                        ctx.clone(),
                        Projection::Columns(projection.clone()),
                        false,
                    )?;
                    readers.insert(projection.clone(), reader.clone());
                    reader
                }
            };
            blocks.push_back(Arc::new(RefreshBlock {
                index: BlockMetaIndex {
                    segment_idx,
                    block_idx,
                },
                meta,
                existing,
                hlls,
                top_n,
                missing,
                projection,
                reader,
            }));
        }
    }
    if blocks.is_empty() {
        return Ok(RefreshPlan {
            specs: Vec::new(),
            blocks,
        });
    }
    // Do not create branches for indexes which are already current on every target block.
    specs.retain(|spec| {
        blocks
            .iter()
            .any(|block| block.missing.contains(&spec.index.name))
    });
    for spec in &mut specs {
        if spec.index.index_type == TableIndexType::Inverted {
            spec.inverted = Some(RefreshInvertedIndex::try_create(&operator, &spec.index).await?);
        }
    }
    let specs = specs.into_iter().map(Arc::new).collect::<Vec<_>>();
    Ok(RefreshPlan { specs, blocks })
}

impl ExistingIndexes {
    async fn read(
        operator: &Operator,
        block: &BlockMeta,
        bloom: bool,
        spatial: bool,
    ) -> Result<Self> {
        let bloom = if bloom {
            bloom_meta(operator, block).await?
        } else {
            None
        };
        let spatial = if spatial {
            match &block.spatial_index_location {
                Some(location) if operator.stat(&location.0).await.is_ok() => {
                    Some(load_spatial_index_meta(operator.clone(), &location.0).await?)
                }
                _ => None,
            }
        } else {
            None
        };
        Ok(Self { bloom, spatial })
    }
}

/// Keep each index type's existing generated checks. Ngram refresh checks the
/// fixed block-derived file, independently of metadata publication.
async fn needs_refresh(
    operator: &Operator,
    block: &BlockMeta,
    index: &TableIndex,
    schema: &TableSchemaRef,
    existing: &ExistingIndexes,
) -> Result<bool> {
    match index.index_type {
        TableIndexType::Inverted => {
            Ok(!block.inverted_index_meta(&index.name).is_some_and(|meta| {
                !meta.location.0.is_empty()
                    && meta.index_version == index.version
                    && meta.location.1 == INVERTED_INDEX_FILE_FORMAT_VERSION
            }))
        }
        TableIndexType::Ngram => {
            let Some(meta) = &existing.bloom else {
                return Ok(true);
            };
            let args = FuseTable::create_ngram_index_args(
                &BTreeMap::from([(index.name.clone(), index.clone())]),
                schema,
                false,
            )?;
            Ok(!args.iter().all(|arg| {
                let name = BloomIndex::build_filter_ngram_name(
                    arg.column_id(),
                    arg.gram_size(),
                    arg.bloom_size(),
                    arg.hash_algorithm(),
                );
                meta.columns.iter().any(|(column, _)| column == &name)
            }))
        }
        TableIndexType::Vector => {
            if let Some(meta) = block
                .vector_index_meta(&index.name)
                .filter(|meta| meta.index_version == index.version)
            {
                return Ok(!vector_index_file_present(operator, meta).await?);
            }
            Ok(true)
        }
        TableIndexType::Spatial => {
            let stats = block.spatial_stats.as_ref();
            // Match the existing refresh contract: require persisted statistics
            // and a current file containing every indexed column. A false
            // validity flag (also the legacy default) does not prove completion.
            let Some(meta) = &existing.spatial else {
                return Ok(true);
            };
            Ok(meta.metadata.get(&index.name) != Some(&index.version)
                || !index.column_ids.iter().all(|id| {
                    meta.columns.iter().any(|(name, _)| name == &id.to_string())
                        && stats.is_some_and(|stats| stats.contains_key(id))
                }))
        }
    }
}

async fn bloom_meta(operator: &Operator, block: &BlockMeta) -> Result<Option<Arc<BloomIndexMeta>>> {
    let path =
        TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(&block.location.0);
    // Match the original Ngram check: stat failures are treated as absent.
    let Ok(stat) = operator.stat(&path).await else {
        return Ok(None);
    };
    let size = stat.content_length();
    let meta = load_index_meta(operator.clone(), &path, size, None).await?;
    Ok(Some(meta))
}

async fn vector_index_file_present(operator: &Operator, meta: &BlockIndexMeta) -> Result<bool> {
    match operator.stat(&meta.location.0).await {
        Ok(stat) => Ok(stat.content_length() == meta.size),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;
    use databend_storages_common_table_meta::meta::BlockIndexMeta;
    use opendal::Operator;
    use opendal::services::Memory;

    use crate::operations::table_index::plan::vector_index_file_present;

    #[tokio::test]
    async fn test_vector_index_file_present() -> Result<()> {
        let operator = Operator::new(Memory::default())?.finish();
        let mut meta = BlockIndexMeta {
            index_name: "vec".into(),
            index_version: "v1".into(),
            location: ("bundle".into(), 1),
            size: 3,
        };
        assert!(!vector_index_file_present(&operator, &meta).await?);
        operator.write("bundle", vec![1, 2, 3]).await?;
        assert!(vector_index_file_present(&operator, &meta).await?);
        meta.size = 4;
        assert!(!vector_index_file_present(&operator, &meta).await?);
        Ok(())
    }
}
