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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::DraftVirtualBlockMeta;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use databend_storages_common_table_meta::meta::VirtualColumnPathStatistics;
use databend_storages_common_table_meta::meta::VirtualSegmentColumnPath;
use databend_storages_common_table_meta::meta::VirtualSegmentPath;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;
use databend_storages_common_table_meta::meta::column_oriented_segment::VirtualBlockInput;

/// Collects virtual paths for one output segment.
///
/// Path indexes and direct column ids are deliberately assigned from scratch in
/// deterministic `(source_column_id, canonical_path)` order. Existing ids are
/// interpreted through their input schema, but are never reused.
#[derive(Default)]
pub struct VirtualSegmentSchemaBuilder {
    /// Paths grouped by source Variant column. `BTreeMap` makes schema and id
    /// assignment deterministic across block orderings.
    sources: BTreeMap<ColumnId, SourcePaths>,
}

#[derive(Default)]
struct SourcePaths {
    /// Canonical paths retained by metadata producers.
    paths: BTreeSet<String>,
}

impl VirtualSegmentSchemaBuilder {
    /// Registers paths actually referenced by one existing block by resolving
    /// the ids found in its direct metadata and path statistics through the old
    /// segment schema.
    pub fn add_existing_block(&mut self, block: &BlockMeta, schema: &VirtualSegmentSchema) {
        if let Some(meta) = &block.virtual_block_meta {
            for column_id in meta.virtual_column_metas.keys() {
                if let Some((source_column_id, path)) = schema.field_of_column_id(*column_id) {
                    let paths = &mut self.sources.entry(source_column_id).or_default().paths;
                    if !paths.contains(path.path.as_str()) {
                        paths.insert(path.path.clone());
                    }
                }
            }
        }
        if let Some(statistics) = &block.virtual_path_statistics {
            for (source_column_id, source) in statistics {
                let source_paths = self.sources.entry(*source_column_id).or_default();
                for (column_id, _) in &source.path_counts {
                    if let Some((path_source_column_id, path)) =
                        schema.field_of_column_id(*column_id)
                        && path_source_column_id == *source_column_id
                    {
                        if !source_paths.paths.contains(path.path.as_str()) {
                            source_paths.paths.insert(path.path.clone());
                        }
                    }
                }
            }
        }
    }

    /// Registers canonical direct paths and path-frequency entries produced for
    /// one new block. Draft metadata has no segment-local ids yet.
    pub fn add_draft_block(&mut self, draft: &DraftVirtualBlockMeta) {
        if let Some(columns) = &draft.virtual_columns {
            for column in &columns.virtual_column_metas {
                let paths = &mut self
                    .sources
                    .entry(column.source_column_id)
                    .or_default()
                    .paths;
                if !paths.contains(column.name.as_str()) {
                    paths.insert(column.name.clone());
                }
            }
        }

        if let Some(statistics) = &draft.path_statistics {
            for (source_column_id, statistics) in statistics {
                let source_paths = self.sources.entry(*source_column_id).or_default();
                for (path, _) in &statistics.path_counts {
                    if !source_paths.paths.contains(path.as_str()) {
                        source_paths.paths.insert(path.clone());
                    }
                }
            }
        }
    }

    /// Builds the deterministic output schema. `BTreeMap` iteration guarantees
    /// source ids and canonical paths are sorted; ids are assigned in that exact
    /// order, so path order and column-id order are identical and contiguous.
    pub fn build(self) -> Option<VirtualSegmentSchema> {
        let mut next_column_id = 0;
        // `BTreeMap` orders source ids and `BTreeSet<String>` orders canonical
        // paths lexicographically by their UTF-8 byte sequences.
        let column_paths = self
            .sources
            .into_iter()
            .map(|(source_column_id, source)| VirtualSegmentColumnPath {
                source_column_id,
                paths: source
                    .paths
                    .into_iter()
                    .map(|path| {
                        let column_id = next_column_id;
                        next_column_id += 1;
                        VirtualSegmentPath { path, column_id }
                    })
                    .collect(),
            })
            .collect();
        let schema = VirtualSegmentSchema { column_paths };
        debug_assert!(
            schema
                .column_paths
                .windows(2)
                .all(|columns| { columns[0].source_column_id < columns[1].source_column_id })
        );
        debug_assert!(schema.column_paths.iter().all(|column| {
            column.paths.windows(2).all(|paths| {
                paths[0].path < paths[1].path && paths[0].column_id < paths[1].column_id
            })
        }));
        (!schema.is_empty()).then_some(schema)
    }
}

/// Builds one deterministic segment-local virtual schema and rewrites all block
/// metadata to use its newly assigned path indexes and direct column ids.
/// `virtual_inputs` must be aligned one-to-one with `blocks` and is consumed.
pub fn build_virtual_segment_schema(
    blocks: &mut [Arc<BlockMeta>],
    virtual_inputs: &mut [VirtualBlockInput],
) -> Result<Option<VirtualSegmentSchema>> {
    if blocks.len() != virtual_inputs.len() {
        return Err(ErrorCode::Internal(format!(
            "virtual metadata input count {} does not match block count {}",
            virtual_inputs.len(),
            blocks.len()
        )));
    }

    if virtual_inputs.iter().all(|input| match input {
        VirtualBlockInput::None => true,
        VirtualBlockInput::Existing { schema } => schema.is_none(),
        VirtualBlockInput::Draft(_) => false,
    }) {
        virtual_inputs.fill(VirtualBlockInput::None);
        return Ok(None);
    }

    // 1. Consume each input descriptor and retain either the existing segment
    // schema needed to decode old ids or the draft metadata for a new block.
    let mut input_schemas = Vec::with_capacity(virtual_inputs.len());
    let mut drafts = Vec::with_capacity(virtual_inputs.len());
    for input in virtual_inputs.iter_mut() {
        match std::mem::replace(input, VirtualBlockInput::None) {
            VirtualBlockInput::None => {
                input_schemas.push(None);
                drafts.push(None);
            }
            VirtualBlockInput::Draft(draft) => {
                input_schemas.push(None);
                drafts.push(Some(draft));
            }
            VirtualBlockInput::Existing { schema } => {
                input_schemas.push(schema.map(Arc::unwrap_or_clone));
                drafts.push(None);
            }
        }
    }

    // 2. Collect only paths referenced by surviving existing blocks, then add
    // all draft paths. The builder assigns fresh deterministic ids afterward.
    let mut builder = VirtualSegmentSchemaBuilder::default();
    for (block, schema) in blocks.iter().zip(&input_schemas) {
        if let Some(schema) = schema {
            builder.add_existing_block(block, schema);
        }
    }
    for draft in drafts.iter().flatten() {
        builder.add_draft_block(draft);
    }

    let Some(output_schema) = builder.build() else {
        return Ok(None);
    };

    // 3. Build the canonical path -> new segment-local id lookup, then
    // materialize draft blocks directly with their final ids.
    let output_column_ids = output_schema
        .column_paths
        .iter()
        .flat_map(|source| {
            source
                .paths
                .iter()
                .map(move |path| ((source.source_column_id, path.path.clone()), path.column_id))
        })
        .collect::<HashMap<_, _>>();

    let draft_blocks = drafts.iter().map(Option::is_some).collect::<Vec<_>>();
    for (block, draft) in blocks.iter_mut().zip(&mut drafts) {
        let Some(draft) = draft.take() else {
            continue;
        };
        let mut path_statistics = HashMap::new();
        for (source_column_id, source) in draft.path_statistics.unwrap_or_default() {
            let mut path_counts = source
                .path_counts
                .into_iter()
                .filter_map(|(path, value_count)| {
                    output_column_ids
                        .get(&(source_column_id, path))
                        .copied()
                        .map(|column_id| (column_id, value_count))
                })
                .collect::<Vec<_>>();
            if !path_counts.is_empty() {
                path_counts.sort_by_key(|(column_id, _)| *column_id);
                path_statistics.insert(source_column_id, VirtualColumnPathStatistics {
                    path_counts,
                    path_statistics_complete: source.path_statistics_complete,
                });
            }
        }

        let mut virtual_column_metas = HashMap::new();
        let block = Arc::make_mut(block);
        if let Some(virtual_columns) = draft.virtual_columns {
            for column in virtual_columns.virtual_column_metas {
                if let Some(column_id) = output_column_ids
                    .get(&(column.source_column_id, column.name))
                    .copied()
                {
                    virtual_column_metas.insert(column_id, column.column_meta);
                }
            }
            block.virtual_block_meta = Some(VirtualBlockMeta {
                virtual_column_metas,
                virtual_column_size: virtual_columns.virtual_column_size,
                virtual_location: virtual_columns.virtual_location,
                virtual_columns_complete: virtual_columns.virtual_columns_complete,
            });
        }
        block.virtual_path_statistics = (!path_statistics.is_empty()).then_some(path_statistics);
    }

    // 4. Rewrite surviving existing blocks from old segment-local ids to the
    // newly assigned ids. Metadata without an input schema stays incomplete.
    for ((block, input_schema), is_draft) in blocks.iter_mut().zip(input_schemas).zip(draft_blocks)
    {
        let block = Arc::make_mut(block);
        let Some(input_schema) = input_schema else {
            if !is_draft {
                // Without the original segment schema, persisted ids cannot be
                // mapped to canonical paths or reassigned safely. Drop all
                // id-based metadata and force footer-based sidecar reads.
                block.virtual_path_statistics = None;
                if let Some(meta) = &mut block.virtual_block_meta {
                    meta.virtual_column_metas.clear();
                    meta.virtual_columns_complete = false;
                }
            }
            continue;
        };

        if let Some(old_statistics) = std::mem::take(&mut block.virtual_path_statistics) {
            let mut new_statistics = HashMap::with_capacity(old_statistics.len());
            for (source_column_id, source) in old_statistics {
                let mut path_statistics_complete = source.path_statistics_complete;
                let mut path_counts = Vec::with_capacity(source.path_counts.len());
                for (old_column_id, value_count) in source.path_counts {
                    let Some((path_source_column_id, path)) =
                        input_schema.field_of_column_id(old_column_id)
                    else {
                        path_statistics_complete = false;
                        continue;
                    };
                    if path_source_column_id != source_column_id {
                        path_statistics_complete = false;
                        continue;
                    }
                    let Some(new_column_id) = output_column_ids
                        .get(&(source_column_id, path.path.clone()))
                        .copied()
                    else {
                        path_statistics_complete = false;
                        continue;
                    };
                    path_counts.push((new_column_id, value_count));
                }
                path_counts.sort_by_key(|(column_id, _)| *column_id);
                if !path_counts.is_empty() || !path_statistics_complete {
                    new_statistics.insert(source_column_id, VirtualColumnPathStatistics {
                        path_statistics_complete,
                        path_counts,
                    });
                }
            }
            block.virtual_path_statistics = (!new_statistics.is_empty()).then_some(new_statistics);
        }

        let Some(meta) = block.virtual_block_meta.as_mut() else {
            continue;
        };
        let old_columns = std::mem::take(&mut meta.virtual_column_metas);
        let mut new_columns = HashMap::<ColumnId, VirtualColumnMeta>::new();
        for (old_column_id, column_meta) in old_columns {
            let path = input_schema
                .field_of_column_id(old_column_id)
                .map(|(source_column_id, path)| (source_column_id, path.path.clone()));
            let Some(new_column_id) = path.and_then(|key| output_column_ids.get(&key).copied())
            else {
                meta.virtual_columns_complete = false;
                continue;
            };
            new_columns.insert(new_column_id, column_meta);
        }
        meta.virtual_column_metas = new_columns;
    }

    Ok(Some(output_schema))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_expression::VIRTUAL_COLUMN_ID_START;
    use databend_storages_common_table_meta::meta::Compression;
    use databend_storages_common_table_meta::meta::DraftVirtualBlockMeta;
    use databend_storages_common_table_meta::meta::DraftVirtualColumnBlockMeta;
    use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
    use databend_storages_common_table_meta::meta::DraftVirtualColumnPathStatistics;
    use databend_storages_common_table_meta::meta::VirtualBlockMeta;
    use databend_storages_common_table_meta::meta::VirtualColumnPathStatistics;
    use databend_storages_common_table_meta::meta::VirtualColumnPhysicalType;
    use databend_storages_common_table_meta::meta::VirtualSegmentColumnPath;
    use databend_storages_common_table_meta::meta::VirtualSegmentPath;
    use databend_storages_common_table_meta::meta::VirtualSegmentSchema;

    use super::*;

    fn dummy_column_meta() -> VirtualColumnMeta {
        VirtualColumnMeta {
            offset: 0,
            len: 8,
            num_values: 1,
            data_type: VirtualColumnPhysicalType::String.encode().0,
            extended_physical_type: None,
            column_stat: None,
        }
    }

    fn dummy_column_meta_with_offset(offset: u64) -> VirtualColumnMeta {
        VirtualColumnMeta {
            offset,
            ..dummy_column_meta()
        }
    }

    fn empty_block() -> Arc<BlockMeta> {
        let mut block = block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::new(),
            virtual_column_size: 0,
            virtual_location: (String::new(), 0),
            virtual_columns_complete: true,
        });
        Arc::make_mut(&mut block).virtual_block_meta = None;
        block
    }

    fn block_with_virtual(meta: VirtualBlockMeta) -> Arc<BlockMeta> {
        Arc::new(BlockMeta {
            row_count: 1,
            block_size: 1,
            file_size: 1,
            col_stats: HashMap::new(),
            col_metas: HashMap::new(),
            cluster_stats: None,
            partition_stats: None,
            location: (String::new(), 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            inverted_index_size: None,
            ngram_filter_index_size: None,
            vector_index_size: None,
            vector_index_location: None,
            spatial_index_size: None,
            spatial_index_location: None,
            spatial_stats: None,
            vector_stats: None,
            virtual_path_statistics: None,
            virtual_block_meta: Some(meta),
            compression: Compression::Lz4,
            create_on: None,
        })
    }

    fn schema_from_paths(
        source_column_id: ColumnId,
        paths: Vec<VirtualSegmentPath>,
    ) -> VirtualSegmentSchema {
        VirtualSegmentSchema {
            column_paths: vec![VirtualSegmentColumnPath {
                source_column_id,
                paths,
            }],
        }
    }

    fn path_stats(
        source_column_id: ColumnId,
        counts: &[(u32, u32)],
    ) -> HashMap<ColumnId, VirtualColumnPathStatistics> {
        HashMap::from([(source_column_id, VirtualColumnPathStatistics {
            path_statistics_complete: true,
            path_counts: counts.to_vec(),
        })])
    }

    #[test]
    fn insert_stats_only_assigns_local_indexes_without_column_ids() {
        let schema = schema_from_paths(1, vec![
            VirtualSegmentPath {
                path: "user.name".to_string(),
                column_id: 0,
            },
            VirtualSegmentPath {
                path: "user.email".to_string(),
                column_id: 1,
            },
        ]);
        let mut block = block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::new(),
            virtual_column_size: 0,
            virtual_location: (String::new(), 0),
            virtual_columns_complete: true,
        });
        Arc::make_mut(&mut block).virtual_path_statistics = Some(path_stats(1, &[(0, 10), (1, 4)]));
        let mut blocks = vec![block];
        let input = vec![Some(schema)];
        let output = build_virtual_segment_schema(
            &mut blocks,
            &mut input
                .into_iter()
                .map(|schema| VirtualBlockInput::Existing {
                    schema: schema.map(Arc::new),
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(output.column_paths.len(), 1);
        assert_eq!(output.column_paths[0].paths[0].path, "user.email");
        assert_eq!(output.column_paths[0].paths[0].column_id, 0);
        assert_eq!(output.column_paths[0].paths[1].path, "user.name");
        let meta = blocks[0].virtual_block_meta.as_ref().unwrap();
        assert_eq!(
            blocks[0].virtual_path_statistics.as_ref().unwrap()[&1].path_counts[0].0,
            0
        );
        assert_eq!(
            blocks[0].virtual_path_statistics.as_ref().unwrap()[&1].path_counts[0].1,
            4
        );
        assert_eq!(
            blocks[0].virtual_path_statistics.as_ref().unwrap()[&1].path_counts[1].0,
            1
        );
        assert_eq!(
            blocks[0].virtual_path_statistics.as_ref().unwrap()[&1].path_counts[1].1,
            10
        );
        assert!(meta.virtual_column_metas.is_empty());
    }

    #[test]
    fn delete_remaining_block_drops_deleted_paths_and_keeps_direct_column() {
        let remaining_schema = schema_from_paths(1, vec![
            VirtualSegmentPath {
                path: "user.city".to_string(),
                column_id: VIRTUAL_COLUMN_ID_START + 7,
            },
            VirtualSegmentPath {
                path: "user.name".to_string(),
                column_id: VIRTUAL_COLUMN_ID_START + 8,
            },
        ]);
        let mut blocks = vec![block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::from([(
                VIRTUAL_COLUMN_ID_START + 8,
                dummy_column_meta(),
            )]),
            virtual_column_size: 8,
            virtual_location: ("vb".to_string(), 0),
            virtual_columns_complete: true,
        })];
        Arc::make_mut(&mut blocks[0]).virtual_path_statistics =
            Some(path_stats(1, &[(VIRTUAL_COLUMN_ID_START + 7, 2)]));
        let input = vec![Some(remaining_schema)];
        let output = build_virtual_segment_schema(
            &mut blocks,
            &mut input
                .into_iter()
                .map(|schema| VirtualBlockInput::Existing {
                    schema: schema.map(Arc::new),
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(output.column_paths[0].paths.len(), 2);
        assert_eq!(output.column_paths[0].paths[0].path, "user.city");
        assert_eq!(output.column_paths[0].paths[0].column_id, 0);
        assert_eq!(output.column_paths[0].paths[1].path, "user.name");
        assert_eq!(output.column_paths[0].paths[1].column_id, 1);
        let meta = blocks[0].virtual_block_meta.as_ref().unwrap();
        assert!(meta.virtual_column_metas.contains_key(&1));
        assert!(
            !meta
                .virtual_column_metas
                .contains_key(&(VIRTUAL_COLUMN_ID_START + 8))
        );
        assert_eq!(
            blocks[0].virtual_path_statistics.as_ref().unwrap()[&1].path_counts,
            vec![(0, 2)]
        );
    }

    #[test]
    fn update_mixes_remaining_old_schema_with_replaced_temp_schema() {
        let remaining_schema = schema_from_paths(1, vec![
            VirtualSegmentPath {
                path: "user.age".to_string(),
                column_id: VIRTUAL_COLUMN_ID_START + 9,
            },
            VirtualSegmentPath {
                path: "user.name".to_string(),
                column_id: VIRTUAL_COLUMN_ID_START + 10,
            },
        ]);
        let replaced_schema = schema_from_paths(1, vec![VirtualSegmentPath {
            path: "user.email".to_string(),
            column_id: 0,
        }]);
        let remaining = block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::from([(
                VIRTUAL_COLUMN_ID_START + 10,
                dummy_column_meta(),
            )]),
            virtual_column_size: 8,
            virtual_location: ("old".to_string(), 0),
            virtual_columns_complete: true,
        });
        let replaced = block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::new(),
            virtual_column_size: 0,
            virtual_location: (String::new(), 0),
            virtual_columns_complete: true,
        });
        let mut blocks = vec![remaining, replaced];
        Arc::make_mut(&mut blocks[0]).virtual_path_statistics =
            Some(path_stats(1, &[(VIRTUAL_COLUMN_ID_START + 9, 3)]));
        Arc::make_mut(&mut blocks[1]).virtual_path_statistics = Some(path_stats(1, &[(0, 7)]));
        let input = vec![Some(remaining_schema), Some(replaced_schema)];
        let output = build_virtual_segment_schema(
            &mut blocks,
            &mut input
                .into_iter()
                .map(|schema| VirtualBlockInput::Existing {
                    schema: schema.map(Arc::new),
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .unwrap();

        let paths = output.column_paths[0]
            .paths
            .iter()
            .map(|path| (path.path.as_str(), path.column_id))
            .collect::<Vec<_>>();
        assert_eq!(paths, vec![
            ("user.age", 0),
            ("user.email", 1),
            ("user.name", 2)
        ]);

        let remaining_meta = blocks[0].virtual_block_meta.as_ref().unwrap();
        assert!(remaining_meta.virtual_column_metas.contains_key(&2));
        assert_eq!(
            blocks[0].virtual_path_statistics.as_ref().unwrap()[&1].path_counts,
            vec![(0, 3)]
        );

        let replaced_meta = blocks[1].virtual_block_meta.as_ref().unwrap();
        assert!(replaced_meta.virtual_column_metas.is_empty());
        assert_eq!(
            blocks[1].virtual_path_statistics.as_ref().unwrap()[&1].path_counts[0].0,
            1
        );
        assert_eq!(
            blocks[1].virtual_path_statistics.as_ref().unwrap()[&1].path_counts[0].1,
            7
        );
    }

    #[test]
    fn recluster_direct_columns_are_dense_and_local() {
        let schema = schema_from_paths(1, vec![VirtualSegmentPath {
            path: "user.name".to_string(),
            column_id: VIRTUAL_COLUMN_ID_START + 3,
        }]);
        let mut blocks = vec![block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::from([(
                VIRTUAL_COLUMN_ID_START + 3,
                dummy_column_meta(),
            )]),
            virtual_column_size: 16,
            virtual_location: ("new".to_string(), 0),
            virtual_columns_complete: true,
        })];
        Arc::make_mut(&mut blocks[0]).virtual_path_statistics = Some(path_stats(1, &[(0, 12)]));
        let input = vec![Some(schema)];
        let output = build_virtual_segment_schema(
            &mut blocks,
            &mut input
                .into_iter()
                .map(|schema| VirtualBlockInput::Existing {
                    schema: schema.map(Arc::new),
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(output.column_paths[0].paths[0].column_id, 0);
        let meta = blocks[0].virtual_block_meta.as_ref().unwrap();
        assert!(meta.virtual_column_metas.contains_key(&0));
        assert_eq!(
            blocks[0].virtual_path_statistics.as_ref().unwrap()[&1].path_counts,
            Vec::<(ColumnId, u32)>::new()
        );
        assert!(!blocks[0].virtual_path_statistics.as_ref().unwrap()[&1].path_statistics_complete);
    }

    #[test]
    fn draft_block_materializes_direct_and_shared_paths() {
        let draft = DraftVirtualBlockMeta {
            virtual_columns: Some(DraftVirtualColumnBlockMeta {
                virtual_column_metas: vec![DraftVirtualColumnMeta {
                    source_column_id: 1,
                    name: "a".to_string(),
                    data_type: VirtualColumnPhysicalType::String,
                    column_meta: dummy_column_meta_with_offset(11),
                }],
                virtual_columns_complete: false,
                virtual_column_size: 8,
                virtual_location: ("draft".to_string(), 0),
            }),
            path_statistics: Some(HashMap::from([(1, DraftVirtualColumnPathStatistics {
                // VirtualColumnBuilder removes direct path `a` before handing
                // draft statistics to segment schema materialization.
                path_counts: vec![("b".to_string(), 3)],
                path_statistics_complete: true,
            })])),
        };
        let mut blocks = vec![empty_block()];
        let mut inputs = vec![VirtualBlockInput::Draft(draft)];

        let output = build_virtual_segment_schema(&mut blocks, &mut inputs)
            .unwrap()
            .unwrap();

        let paths = &output.column_paths[0].paths;
        assert_eq!(paths[0].path, "a");
        assert_eq!(paths[0].column_id, 0);
        assert_eq!(paths[1].path, "b");
        assert_eq!(paths[1].column_id, 1);
        assert_eq!(
            blocks[0]
                .virtual_block_meta
                .as_ref()
                .unwrap()
                .virtual_column_metas[&0]
                .offset,
            11
        );
        assert_eq!(
            blocks[0].virtual_path_statistics.as_ref().unwrap()[&1].path_counts,
            vec![(1, 3)]
        );
        assert!(matches!(inputs[0], VirtualBlockInput::None));
    }

    #[test]
    fn conflicting_existing_column_ids_are_reassigned_by_path() {
        let old_id = VIRTUAL_COLUMN_ID_START + 5;
        let schemas = [
            schema_from_paths(1, vec![VirtualSegmentPath {
                path: "b".to_string(),
                column_id: old_id,
            }]),
            schema_from_paths(1, vec![VirtualSegmentPath {
                path: "a".to_string(),
                column_id: old_id,
            }]),
        ];
        let mut blocks = vec![
            block_with_virtual(VirtualBlockMeta {
                virtual_column_metas: HashMap::from([(old_id, dummy_column_meta_with_offset(22))]),
                virtual_column_size: 8,
                virtual_location: ("b".to_string(), 0),
                virtual_columns_complete: true,
            }),
            block_with_virtual(VirtualBlockMeta {
                virtual_column_metas: HashMap::from([(old_id, dummy_column_meta_with_offset(11))]),
                virtual_column_size: 8,
                virtual_location: ("a".to_string(), 0),
                virtual_columns_complete: true,
            }),
        ];
        let mut inputs = schemas
            .into_iter()
            .map(|schema| VirtualBlockInput::Existing {
                schema: Some(Arc::new(schema)),
            })
            .collect::<Vec<_>>();

        let output = build_virtual_segment_schema(&mut blocks, &mut inputs)
            .unwrap()
            .unwrap();

        assert_eq!(
            output.column_paths[0]
                .paths
                .iter()
                .map(|path| (path.path.as_str(), path.column_id))
                .collect::<Vec<_>>(),
            vec![("a", 0), ("b", 1)]
        );
        assert_eq!(
            blocks[0]
                .virtual_block_meta
                .as_ref()
                .unwrap()
                .virtual_column_metas[&1]
                .offset,
            22
        );
        assert_eq!(
            blocks[1]
                .virtual_block_meta
                .as_ref()
                .unwrap()
                .virtual_column_metas[&0]
                .offset,
            11
        );
    }

    #[test]
    fn empty_and_misaligned_inputs_are_handled() {
        let mut blocks = vec![empty_block()];
        let err = build_virtual_segment_schema(&mut blocks, &mut []).unwrap_err();
        assert!(err.message().contains("does not match block count"));

        let mut inputs = vec![VirtualBlockInput::None];
        assert!(
            build_virtual_segment_schema(&mut blocks, &mut inputs)
                .unwrap()
                .is_none()
        );
    }
}
