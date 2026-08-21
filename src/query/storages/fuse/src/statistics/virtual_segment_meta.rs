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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::VariantDataType;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use databend_storages_common_table_meta::meta::VirtualColumnPathCount;
use databend_storages_common_table_meta::meta::VirtualColumnPathStatistics;
use databend_storages_common_table_meta::meta::VirtualSegmentColumnPath;
use databend_storages_common_table_meta::meta::VirtualSegmentPath;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;

#[derive(Default)]
struct VirtualSegmentSchemaBuilder {
    columns: BTreeMap<ColumnId, ColumnBuildState>,
}

#[derive(Default)]
struct ColumnBuildState {
    paths: BTreeMap<String, PathBuildState>,
    path_statistics_complete: bool,
}

impl ColumnBuildState {
    fn new() -> Self {
        Self {
            paths: BTreeMap::new(),
            path_statistics_complete: true,
        }
    }
}

#[derive(Default)]
struct PathBuildState {
    data_types: Vec<VariantDataType>,
    is_direct: bool,
}

impl VirtualSegmentSchemaBuilder {
    fn add_stat(&mut self, source_column_id: ColumnId, path: String, _value_count: u64) {
        if path.is_empty() {
            return;
        }
        self.columns
            .entry(source_column_id)
            .or_insert_with(ColumnBuildState::new)
            .paths
            .entry(path)
            .or_default();
    }

    fn add_direct(
        &mut self,
        source_column_id: ColumnId,
        path: String,
        data_types: Vec<VariantDataType>,
    ) {
        if path.is_empty() {
            return;
        }
        let state = self
            .columns
            .entry(source_column_id)
            .or_insert_with(ColumnBuildState::new)
            .paths
            .entry(path)
            .or_default();
        state.is_direct = true;
        for data_type in data_types {
            if !state.data_types.contains(&data_type) {
                state.data_types.push(data_type);
            }
        }
    }

    fn and_complete(&mut self, source_column_id: ColumnId, complete: bool) {
        self.columns
            .entry(source_column_id)
            .or_insert_with(ColumnBuildState::new)
            .path_statistics_complete &= complete;
    }

    fn build(self) -> VirtualSegmentSchema {
        let mut next_column_id = 0;
        let column_paths = self
            .columns
            .into_iter()
            .map(|(source_column_id, column)| VirtualSegmentColumnPath {
                source_column_id,
                paths: column
                    .paths
                    .into_iter()
                    .map(|(path, mut state)| {
                        state.data_types.sort();
                        state.data_types.dedup();
                        let column_id = state.is_direct.then(|| {
                            let id = next_column_id;
                            next_column_id += 1;
                            id
                        });
                        VirtualSegmentPath {
                            path,
                            column_id,
                            data_types: state.data_types,
                        }
                    })
                    .collect(),
                path_statistics_complete: column.path_statistics_complete,
            })
            .collect();
        VirtualSegmentSchema { column_paths }
    }
}

/// Rebuilds a deterministic segment-local virtual schema and rewrites every
/// block's segment-local column/path ids to match it. `input_schemas` is aligned
/// with `blocks` and describes the ids currently used by each block.
pub fn rebuild_virtual_segment_meta(
    blocks: &mut [Arc<BlockMeta>],
    input_schemas: &[Option<VirtualSegmentSchema>],
) -> Result<Option<VirtualSegmentSchema>> {
    if blocks.len() != input_schemas.len() {
        return Err(ErrorCode::Internal(format!(
            "virtual schema count {} does not match block count {}",
            input_schemas.len(),
            blocks.len()
        )));
    }

    let mut builder = VirtualSegmentSchemaBuilder::default();
    let mut has_paths = false;
    for (block, schema) in blocks.iter().zip(input_schemas) {
        let Some(meta) = &block.virtual_block_meta else {
            continue;
        };
        let Some(schema) = schema else {
            continue;
        };
        has_paths = true;
        for column in &schema.column_paths {
            builder.and_complete(column.source_column_id, column.path_statistics_complete);
        }
        for source in &meta.path_statistics {
            for stat in &source.paths {
                if let Some(path) = schema.path(source.source_column_id, stat.path_index) {
                    builder.add_stat(source.source_column_id, path.path.clone(), stat.value_count);
                }
            }
        }
        for column_id in meta.virtual_column_metas.keys() {
            let Some((source_column_id, path)) = schema.field_of_column_id(*column_id) else {
                continue;
            };
            builder.add_direct(source_column_id, path.path.clone(), path.data_types.clone());
        }
    }

    if !has_paths {
        return Ok(None);
    }

    let output_schema = builder.build();
    if output_schema.is_empty() {
        return Ok(None);
    }

    let mut output_path_indexes = HashMap::<(ColumnId, String), u32>::new();
    let mut output_column_ids = HashMap::<(ColumnId, String), ColumnId>::new();
    for column in &output_schema.column_paths {
        for (path_index, path) in column.paths.iter().enumerate() {
            let key = (column.source_column_id, path.path.clone());
            output_path_indexes.insert(key.clone(), path_index as u32);
            if let Some(column_id) = path.column_id {
                output_column_ids.insert(key, column_id);
            }
        }
    }

    for (block, input_schema) in blocks.iter_mut().zip(input_schemas) {
        let Some(meta) = Arc::make_mut(block).virtual_block_meta.as_mut() else {
            continue;
        };
        let Some(input_schema) = input_schema else {
            meta.virtual_columns_complete = false;
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

        let old_statistics = std::mem::take(&mut meta.path_statistics);
        let mut grouped = HashMap::<ColumnId, (bool, Vec<VirtualColumnPathCount>)>::new();
        for source in old_statistics {
            let entry = grouped
                .entry(source.source_column_id)
                .or_insert_with(|| (true, Vec::new()));
            entry.0 &= source.path_statistics_complete;
            for statistic in source.paths {
                let Some(path) = input_schema.path(source.source_column_id, statistic.path_index)
                else {
                    meta.virtual_columns_complete = false;
                    continue;
                };
                let Some(path_index) = output_path_indexes
                    .get(&(source.source_column_id, path.path.clone()))
                    .copied()
                else {
                    meta.virtual_columns_complete = false;
                    continue;
                };
                entry.1.push(VirtualColumnPathCount {
                    path_index,
                    value_count: statistic.value_count,
                });
            }
        }
        let mut new_statistics = grouped
            .into_iter()
            .map(
                |(source_column_id, (path_statistics_complete, mut paths))| {
                    paths.sort_by_key(|stat| stat.path_index);
                    VirtualColumnPathStatistics {
                        source_column_id,
                        path_statistics_complete,
                        paths,
                    }
                },
            )
            .collect::<Vec<_>>();
        new_statistics.sort_by_key(|stat| stat.source_column_id);
        meta.path_statistics = new_statistics;
    }

    Ok(Some(output_schema))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_expression::VIRTUAL_COLUMN_ID_START;
    use databend_common_expression::VariantDataType;
    use databend_storages_common_table_meta::meta::Compression;
    use databend_storages_common_table_meta::meta::VirtualBlockMeta;
    use databend_storages_common_table_meta::meta::VirtualColumnPathStatistics;
    use databend_storages_common_table_meta::meta::VirtualSegmentColumnPath;
    use databend_storages_common_table_meta::meta::VirtualSegmentPath;
    use databend_storages_common_table_meta::meta::VirtualSegmentSchema;

    use super::*;

    fn dummy_column_meta() -> VirtualColumnMeta {
        VirtualColumnMeta {
            offset: 0,
            len: 8,
            num_values: 1,
            data_type: VirtualColumnMeta::data_type_code(&VariantDataType::String),
            column_stat: None,
        }
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
                path_statistics_complete: true,
            }],
        }
    }

    fn path_stats(
        source_column_id: ColumnId,
        counts: &[(u32, u64)],
    ) -> Vec<VirtualColumnPathStatistics> {
        vec![VirtualColumnPathStatistics {
            source_column_id,
            path_statistics_complete: true,
            paths: counts
                .iter()
                .map(|(path_index, value_count)| VirtualColumnPathCount {
                    path_index: *path_index,
                    value_count: *value_count,
                })
                .collect(),
        }]
    }

    #[test]
    fn insert_stats_only_assigns_local_indexes_without_column_ids() {
        let schema = VirtualSegmentSchema::from_pending_paths(
            [
                (1, "user.name".to_string(), None),
                (1, "user.email".to_string(), None),
            ],
            true,
        );
        let mut blocks = vec![block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::new(),
            virtual_column_size: 0,
            virtual_location: (String::new(), 0),
            path_statistics: path_stats(1, &[(0, 10), (1, 4)]),
            virtual_columns_complete: true,
        })];
        let input = vec![Some(schema)];
        let output = rebuild_virtual_segment_meta(&mut blocks, &input)
            .unwrap()
            .unwrap();

        assert_eq!(output.column_paths.len(), 1);
        assert_eq!(output.column_paths[0].paths[0].path, "user.email");
        assert_eq!(output.column_paths[0].paths[0].column_id, None);
        assert_eq!(output.column_paths[0].paths[1].path, "user.name");
        let meta = blocks[0].virtual_block_meta.as_ref().unwrap();
        assert_eq!(meta.path_statistics[0].paths[0].path_index, 0);
        assert_eq!(meta.path_statistics[0].paths[0].value_count, 4);
        assert_eq!(meta.path_statistics[0].paths[1].path_index, 1);
        assert_eq!(meta.path_statistics[0].paths[1].value_count, 10);
        assert!(meta.virtual_column_metas.is_empty());
    }

    #[test]
    fn delete_remaining_block_drops_deleted_paths_and_keeps_direct_column() {
        let remaining_schema = schema_from_paths(1, vec![
            VirtualSegmentPath {
                path: "user.name".to_string(),
                column_id: Some(VIRTUAL_COLUMN_ID_START + 7),
                data_types: vec![VariantDataType::String],
            },
            VirtualSegmentPath {
                path: "user.city".to_string(),
                column_id: None,
                data_types: vec![],
            },
        ]);
        let mut blocks = vec![block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::from([(
                VIRTUAL_COLUMN_ID_START + 7,
                dummy_column_meta(),
            )]),
            virtual_column_size: 8,
            virtual_location: ("vb".to_string(), 0),
            path_statistics: path_stats(1, &[(0, 8), (1, 2)]),
            virtual_columns_complete: true,
        })];
        let input = vec![Some(remaining_schema)];
        let output = rebuild_virtual_segment_meta(&mut blocks, &input)
            .unwrap()
            .unwrap();

        assert_eq!(output.column_paths[0].paths.len(), 2);
        assert_eq!(output.column_paths[0].paths[0].path, "user.city");
        assert_eq!(output.column_paths[0].paths[0].column_id, None);
        assert_eq!(output.column_paths[0].paths[1].path, "user.name");
        assert_eq!(output.column_paths[0].paths[1].column_id, Some(0));
        let meta = blocks[0].virtual_block_meta.as_ref().unwrap();
        assert!(meta.virtual_column_metas.contains_key(&0));
        assert!(
            !meta
                .virtual_column_metas
                .contains_key(&(VIRTUAL_COLUMN_ID_START + 7))
        );
        assert_eq!(meta.path_statistics[0].paths[0].path_index, 0);
        assert_eq!(meta.path_statistics[0].paths[1].path_index, 1);
    }

    #[test]
    fn update_mixes_remaining_old_schema_with_replaced_temp_schema() {
        let remaining_schema = schema_from_paths(1, vec![
            VirtualSegmentPath {
                path: "user.name".to_string(),
                column_id: Some(VIRTUAL_COLUMN_ID_START + 9),
                data_types: vec![VariantDataType::String],
            },
            VirtualSegmentPath {
                path: "user.age".to_string(),
                column_id: None,
                data_types: vec![],
            },
        ]);
        let replaced_schema =
            VirtualSegmentSchema::from_pending_paths([(1, "user.email".to_string(), None)], true);
        let remaining = block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::from([(
                VIRTUAL_COLUMN_ID_START + 9,
                dummy_column_meta(),
            )]),
            virtual_column_size: 8,
            virtual_location: ("old".to_string(), 0),
            path_statistics: path_stats(1, &[(0, 5), (1, 3)]),
            virtual_columns_complete: true,
        });
        let replaced = block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::new(),
            virtual_column_size: 0,
            virtual_location: (String::new(), 0),
            path_statistics: path_stats(1, &[(0, 7)]),
            virtual_columns_complete: true,
        });
        let mut blocks = vec![remaining, replaced];
        let input = vec![Some(remaining_schema), Some(replaced_schema)];
        let output = rebuild_virtual_segment_meta(&mut blocks, &input)
            .unwrap()
            .unwrap();

        let paths = output.column_paths[0]
            .paths
            .iter()
            .map(|path| (path.path.as_str(), path.column_id))
            .collect::<Vec<_>>();
        assert_eq!(paths, vec![
            ("user.age", None),
            ("user.email", None),
            ("user.name", Some(0)),
        ]);

        let remaining_meta = blocks[0].virtual_block_meta.as_ref().unwrap();
        assert!(remaining_meta.virtual_column_metas.contains_key(&0));
        assert_eq!(remaining_meta.path_statistics[0].paths[0].path_index, 0);
        assert_eq!(remaining_meta.path_statistics[0].paths[0].value_count, 3);
        assert_eq!(remaining_meta.path_statistics[0].paths[1].path_index, 2);
        assert_eq!(remaining_meta.path_statistics[0].paths[1].value_count, 5);

        let replaced_meta = blocks[1].virtual_block_meta.as_ref().unwrap();
        assert!(replaced_meta.virtual_column_metas.is_empty());
        assert_eq!(replaced_meta.path_statistics[0].paths[0].path_index, 1);
        assert_eq!(replaced_meta.path_statistics[0].paths[0].value_count, 7);
    }

    #[test]
    fn recluster_direct_columns_are_dense_and_local() {
        let schema = VirtualSegmentSchema::from_pending_paths(
            [(
                1,
                "user.name".to_string(),
                Some((VIRTUAL_COLUMN_ID_START + 3, vec![VariantDataType::String])),
            )],
            true,
        );
        let mut blocks = vec![block_with_virtual(VirtualBlockMeta {
            virtual_column_metas: HashMap::from([(
                VIRTUAL_COLUMN_ID_START + 3,
                dummy_column_meta(),
            )]),
            virtual_column_size: 16,
            virtual_location: ("new".to_string(), 0),
            path_statistics: path_stats(1, &[(0, 12)]),
            virtual_columns_complete: true,
        })];
        let input = vec![Some(schema)];
        let output = rebuild_virtual_segment_meta(&mut blocks, &input)
            .unwrap()
            .unwrap();

        assert_eq!(output.column_paths[0].paths[0].column_id, Some(0));
        let meta = blocks[0].virtual_block_meta.as_ref().unwrap();
        assert!(meta.virtual_column_metas.contains_key(&0));
        assert_eq!(meta.path_statistics[0].paths[0].path_index, 0);
    }
}
