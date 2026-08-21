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

use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::VIRTUAL_COLUMNS_LIMIT;
use databend_common_expression::VirtualDataField;
use databend_common_expression::VirtualDataSchema;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnPathStatistics;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use databend_storages_common_table_meta::meta::VirtualColumnPath;
use databend_storages_common_table_meta::meta::VirtualColumnPathCount;
use databend_storages_common_table_meta::meta::VirtualColumnPathStatistics;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;
use databend_storages_common_table_meta::meta::column_oriented_segment::*;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::encoded_path_from_bracket_name;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;

#[derive(Default)]
pub struct RowOrientedSegmentBuilder {
    pub blocks_metas: Vec<Arc<BlockMeta>>,
    pub virtual_schema: Option<VirtualSegmentSchema>,
    pub virtual_paths: Vec<VirtualColumnPath>,
}

impl SegmentBuilder for RowOrientedSegmentBuilder {
    type Segment = SegmentInfo;
    fn block_count(&self) -> usize {
        self.blocks_metas.len()
    }

    fn add_block(&mut self, block_meta: BlockMeta) -> Result<()> {
        self.blocks_metas.push(Arc::new(block_meta));
        Ok(())
    }

    fn set_virtual_metadata(
        &mut self,
        virtual_schema: Option<VirtualSegmentSchema>,
        virtual_paths: Vec<VirtualColumnPath>,
    ) {
        self.virtual_schema = virtual_schema;
        self.virtual_paths = virtual_paths;
    }

    fn build(
        &mut self,
        thresholds: BlockThresholds,
        cluster_key_info: Option<&ClusterKeyInfo>,
        additional_stats_meta: Option<AdditionalStatsMeta>,
    ) -> Result<Self::Segment> {
        let mut builder = std::mem::take(self);
        let mut stat =
            super::reduce_block_metas(&builder.blocks_metas, thresholds, cluster_key_info)?;
        stat.additional_stats_meta = additional_stats_meta;
        let input_schema = builder.virtual_schema.clone().or_else(|| {
            (!builder.virtual_paths.is_empty()).then(|| {
                let mut schema = VirtualSegmentSchema::from_pending_paths(
                    builder
                        .virtual_paths
                        .iter()
                        .map(|path| (path.source_column_id, path.path.clone(), None)),
                    true,
                );
                for column in &mut schema.column_paths {
                    column.path_statistics_complete = builder
                        .blocks_metas
                        .iter()
                        .filter_map(|block| block.virtual_block_meta.as_ref())
                        .filter_map(|meta| {
                            meta.path_statistics
                                .iter()
                                .find(|stats| stats.source_column_id == column.source_column_id)
                        })
                        .all(|stats| stats.path_statistics_complete);
                }
                schema
            })
        });
        let input_schemas = vec![input_schema; builder.blocks_metas.len()];
        let virtual_schema =
            super::rebuild_virtual_segment_meta(&mut builder.blocks_metas, &input_schemas)?;
        let mut segment = SegmentInfo::new(builder.blocks_metas, stat);
        if virtual_schema
            .as_ref()
            .is_some_and(|schema| !schema.is_empty())
        {
            segment.summary.virtual_segment_schema = virtual_schema;
        }
        Ok(segment)
    }

    fn new(_table_schema: TableSchemaRef, _block_per_segment: usize) -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct VirtualColumnAccumulator {
    virtual_fields: BTreeMap<(ColumnId, String), usize>,
    virtual_schema: VirtualDataSchema,
    number_of_blocks: u64,
    segment_field_indexes: BTreeMap<usize, ()>,
    segment_path_indexes: BTreeMap<VirtualColumnPath, u32>,
    segment_paths: Vec<VirtualColumnPath>,
}

impl VirtualColumnAccumulator {
    pub fn try_create(
        schema: &Arc<TableSchema>,
        virtual_schema: &Option<VirtualDataSchema>,
    ) -> Option<VirtualColumnAccumulator> {
        let has_variant = schema
            .fields
            .iter()
            .any(|f| matches!(f.data_type.remove_nullable(), TableDataType::Variant));
        if !has_variant {
            return None;
        }

        let mut virtual_schema = if let Some(virtual_schema) = virtual_schema {
            virtual_schema.clone()
        } else {
            VirtualDataSchema::empty()
        };

        if virtual_schema.fields.len() > VIRTUAL_COLUMNS_LIMIT {
            virtual_schema.fields.truncate(VIRTUAL_COLUMNS_LIMIT);
        }

        let mut virtual_fields = BTreeMap::new();
        for (i, virtual_field) in virtual_schema.fields.iter().enumerate() {
            let key = (virtual_field.source_column_id, virtual_field.name.clone());
            virtual_fields.insert(key, i);
        }

        Some(VirtualColumnAccumulator {
            virtual_fields,
            virtual_schema,
            number_of_blocks: 0,
            segment_field_indexes: BTreeMap::new(),
            segment_path_indexes: BTreeMap::new(),
            segment_paths: Vec::new(),
        })
    }

    pub fn add_virtual_column_metas(
        &mut self,
        draft_virtual_column_metas: &Vec<DraftVirtualColumnMeta>,
    ) -> HashMap<ColumnId, VirtualColumnMeta> {
        let mut virtual_column_metas = HashMap::new();

        for draft_virtual_column_meta in draft_virtual_column_metas {
            let key = (
                draft_virtual_column_meta.source_column_id,
                draft_virtual_column_meta.name.clone(),
            );

            let column_id = if let Some(field_idx) = self.virtual_fields.get(&key) {
                self.segment_field_indexes.insert(*field_idx, ());
                let virtual_field =
                    unsafe { self.virtual_schema.fields.get_unchecked_mut(*field_idx) };
                if !virtual_field
                    .data_types
                    .contains(&draft_virtual_column_meta.data_type)
                {
                    virtual_field
                        .data_types
                        .push(draft_virtual_column_meta.data_type.clone());
                }
                virtual_field.column_id
            } else {
                if self.virtual_schema.is_full() {
                    continue;
                }
                let field_idx = self.virtual_schema.num_fields();
                self.virtual_fields.insert(key, field_idx);
                self.segment_field_indexes.insert(field_idx, ());

                let new_virtual_field = VirtualDataField {
                    name: draft_virtual_column_meta.name.clone(),
                    data_types: vec![draft_virtual_column_meta.data_type.clone()],
                    source_column_id: draft_virtual_column_meta.source_column_id,
                    column_id: 0,
                };
                self.virtual_schema.add_field(new_virtual_field).unwrap()
            };
            virtual_column_metas.insert(column_id, draft_virtual_column_meta.column_meta.clone());
        }
        self.number_of_blocks += 1;

        virtual_column_metas
    }

    pub fn add_path_statistics(
        &mut self,
        draft_statistics: &[DraftVirtualColumnPathStatistics],
    ) -> Vec<VirtualColumnPathStatistics> {
        let mut path_statistics = Vec::new();
        for source in draft_statistics {
            let mut paths = Vec::with_capacity(source.paths.len());
            for path in &source.paths {
                let key = VirtualColumnPath {
                    source_column_id: source.source_column_id,
                    path: path.path.clone(),
                };
                let path_index = if let Some(path_index) = self.segment_path_indexes.get(&key) {
                    *path_index
                } else {
                    let path_index = self
                        .segment_paths
                        .iter()
                        .filter(|item| item.source_column_id == source.source_column_id)
                        .count() as u32;
                    self.segment_path_indexes.insert(key.clone(), path_index);
                    self.segment_paths.push(key);
                    path_index
                };
                paths.push(VirtualColumnPathCount {
                    path_index,
                    value_count: path.value_count,
                });
            }
            if !paths.is_empty() {
                path_statistics.push(VirtualColumnPathStatistics {
                    source_column_id: source.source_column_id,
                    path_statistics_complete: source.path_statistics_complete,
                    paths,
                });
            }
        }
        path_statistics
    }

    pub fn take_segment_metadata(
        &mut self,
    ) -> (Option<VirtualSegmentSchema>, Vec<VirtualColumnPath>) {
        let legacy_fields = std::mem::take(&mut self.segment_field_indexes)
            .into_keys()
            .filter_map(|index| self.virtual_schema.fields.get(index).cloned())
            .collect::<Vec<_>>();
        let paths = std::mem::take(&mut self.segment_paths);
        self.segment_path_indexes.clear();

        let schema = VirtualSegmentSchema::from_pending_paths(
            paths.iter().map(|path| {
                let column = legacy_fields.iter().find(|field| {
                    field.source_column_id == path.source_column_id
                        && encoded_path_from_bracket_name(&field.name).as_deref()
                            == Some(path.path.as_str())
                });
                (
                    path.source_column_id,
                    path.path.clone(),
                    column.map(|field| (field.column_id, field.data_types.clone())),
                )
            }),
            true,
        );
        let schema = (!schema.is_empty()).then_some(schema);
        (schema, paths)
    }

    pub fn take_segment_paths(&mut self) -> Vec<VirtualColumnPath> {
        self.segment_path_indexes.clear();
        std::mem::take(&mut self.segment_paths)
    }

    pub fn build_virtual_schema(self) -> Option<VirtualDataSchema> {
        if self.virtual_schema.num_fields() > 0 {
            Some(self.virtual_schema)
        } else {
            None
        }
    }

    pub fn build_virtual_schema_with_block_number(mut self) -> Option<VirtualDataSchema> {
        if self.virtual_schema.num_fields() > 0 {
            self.virtual_schema.number_of_blocks += self.number_of_blocks;
            Some(self.virtual_schema)
        } else {
            None
        }
    }
}

#[derive(Default)]
pub struct ColumnHLLAccumulator {
    pub hlls: Vec<RawBlockHLL>,
    pub summary: BlockHLL,
}

impl ColumnHLLAccumulator {
    pub fn add_hll(&mut self, hll: BlockHLLState) -> Result<()> {
        match hll {
            BlockHLLState::Deserialized(v) => {
                let data = encode_column_hll(&v)?;
                self.hlls.push(data);
                merge_column_hll_mut(&mut self.summary, &v);
            }
            BlockHLLState::Serialized(v) => self.hlls.push(v),
        }
        Ok(())
    }

    pub fn build_segment_statistics(&mut self, block_top_ns: Vec<BlockTopN>) -> SegmentStatistics {
        SegmentStatistics::new(std::mem::take(&mut self.hlls), block_top_ns)
    }

    pub fn is_empty(&self) -> bool {
        self.hlls.is_empty()
    }

    pub fn take_summary(&mut self) -> BlockHLL {
        std::mem::take(&mut self.summary)
    }
}
