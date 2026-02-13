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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::VirtualDataField;
use databend_common_expression::VirtualDataSchema;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_storages_common_table_meta::meta::merge_column_hll;

use crate::operations::CommitMeta;
use crate::operations::ConflictResolveContext;
use crate::operations::SnapshotChanges;
use crate::operations::VirtualSchemaMode;
use crate::statistics::merge_statistics;

pub struct TransformMergeCommitMeta {
    to_merged: Vec<CommitMeta>,
    default_cluster_key_id: Option<u32>,
}

impl TransformMergeCommitMeta {
    pub fn create(default_cluster_key_id: Option<u32>) -> Self {
        TransformMergeCommitMeta {
            to_merged: vec![],
            default_cluster_key_id,
        }
    }

    fn merge_conflict_resolve_context(
        l: ConflictResolveContext,
        r: ConflictResolveContext,
        default_cluster_key_id: Option<u32>,
    ) -> ConflictResolveContext {
        match (l, r) {
            (
                ConflictResolveContext::ModifiedSegmentExistsInLatest(l),
                ConflictResolveContext::ModifiedSegmentExistsInLatest(r),
            ) => {
                assert!(!l.check_intersect(&r));

                ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
                    removed_segment_indexes: l
                        .removed_segment_indexes
                        .into_iter()
                        .chain(r.removed_segment_indexes)
                        .collect(),
                    removed_statistics: merge_statistics(
                        l.removed_statistics.clone(),
                        &r.removed_statistics,
                        default_cluster_key_id,
                    ),
                    appended_segments: l
                        .appended_segments
                        .into_iter()
                        .chain(r.appended_segments)
                        .collect(),
                    replaced_segments: l
                        .replaced_segments
                        .into_iter()
                        .chain(r.replaced_segments)
                        .collect(),
                    merged_statistics: merge_statistics(
                        l.merged_statistics.clone(),
                        &r.merged_statistics,
                        default_cluster_key_id,
                    ),
                })
            }
            _ => unreachable!(
                "conflict resolve context to be merged should both be ModifiedSegmentExistsInLatest"
            ),
        }
    }

    pub(crate) fn merge_virtual_schema(
        l_virtual_schema: Option<VirtualDataSchema>,
        r_virtual_schema: Option<VirtualDataSchema>,
    ) -> Option<VirtualDataSchema> {
        match (l_virtual_schema, r_virtual_schema) {
            (Some(l_schema), Some(r_schema)) => {
                let mut merged_fields: Vec<VirtualDataField> = Vec::new();
                let mut l_cursor = 0;
                let mut r_cursor = 0;

                let next_column_id = if l_schema.next_column_id > r_schema.next_column_id {
                    l_schema.next_column_id
                } else {
                    r_schema.next_column_id
                };
                // TODO: Calculate the correct `number_of_blocks`
                let number_of_blocks = if l_schema.number_of_blocks > r_schema.number_of_blocks {
                    l_schema.number_of_blocks
                } else {
                    r_schema.number_of_blocks
                };

                while l_cursor < l_schema.fields.len() || r_cursor < r_schema.fields.len() {
                    match (l_schema.fields.get(l_cursor), r_schema.fields.get(r_cursor)) {
                        (Some(l_field), Some(r_field)) => {
                            if l_field.column_id < r_field.column_id {
                                merged_fields.push(l_field.clone());
                                l_cursor += 1;
                            } else if l_field.column_id > r_field.column_id {
                                merged_fields.push(r_field.clone());
                                r_cursor += 1;
                            } else {
                                // If column_id, source_column_id and name are same, we can merge the field,
                                // otherwise there is a conflict in the column_id and we need to remove the field.
                                if l_field.source_column_id == r_field.source_column_id
                                    && l_field.name == r_field.name
                                {
                                    let mut combined_data_types = BTreeSet::new();
                                    for dt in &l_field.data_types {
                                        combined_data_types.insert(dt.clone());
                                    }
                                    for dt in &r_field.data_types {
                                        combined_data_types.insert(dt.clone());
                                    }
                                    let mut merged_field = l_field.clone();
                                    merged_field.data_types =
                                        combined_data_types.into_iter().collect();
                                    merged_fields.push(merged_field);
                                }
                                l_cursor += 1;
                                r_cursor += 1;
                            }
                        }
                        (Some(l_field), None) => {
                            merged_fields.push(l_field.clone());
                            l_cursor += 1;
                        }
                        (None, Some(r_field)) => {
                            merged_fields.push(r_field.clone());
                            r_cursor += 1;
                        }
                        (None, None) => break,
                    }
                }

                let merged_virtual_schema = VirtualDataSchema {
                    fields: merged_fields,
                    metadata: BTreeMap::new(),
                    next_column_id,
                    number_of_blocks,
                };
                Some(merged_virtual_schema)
            }
            (Some(l_virtual_schema), None) => Some(l_virtual_schema),
            (None, Some(r_virtual_schema)) => Some(r_virtual_schema),
            (None, None) => None,
        }
    }

    pub(crate) fn apply_virtual_schema(
        old_virtual_schema: Option<VirtualDataSchema>,
        new_virtual_schema: Option<VirtualDataSchema>,
        mode: VirtualSchemaMode,
    ) -> Option<VirtualDataSchema> {
        match mode {
            VirtualSchemaMode::Merge => {
                Self::merge_virtual_schema(old_virtual_schema, new_virtual_schema)
            }
            VirtualSchemaMode::Replace => new_virtual_schema,
        }
    }

    pub fn merge_commit_meta(
        l: CommitMeta,
        r: CommitMeta,
        default_cluster_key_id: Option<u32>,
    ) -> CommitMeta {
        assert_eq!(l.table_id, r.table_id, "table id mismatch");
        let (virtual_schema, virtual_schema_mode) =
            match (l.virtual_schema_mode, r.virtual_schema_mode) {
                (_, VirtualSchemaMode::Replace) => (
                    r.virtual_schema.or(l.virtual_schema),
                    VirtualSchemaMode::Replace,
                ),
                (VirtualSchemaMode::Replace, _) => (
                    l.virtual_schema.or(r.virtual_schema),
                    VirtualSchemaMode::Replace,
                ),
                (VirtualSchemaMode::Merge, VirtualSchemaMode::Merge) => (
                    Self::merge_virtual_schema(l.virtual_schema, r.virtual_schema),
                    VirtualSchemaMode::Merge,
                ),
            };

        CommitMeta {
            conflict_resolve_context: Self::merge_conflict_resolve_context(
                l.conflict_resolve_context,
                r.conflict_resolve_context,
                default_cluster_key_id,
            ),
            new_segment_locs: l
                .new_segment_locs
                .into_iter()
                .chain(r.new_segment_locs)
                .collect(),
            table_id: l.table_id,
            virtual_schema,
            virtual_schema_mode,
            hll: merge_column_hll(l.hll, r.hll),
        }
    }
}

impl AccumulatingTransform for TransformMergeCommitMeta {
    const NAME: &'static str = "TransformMergeCommitMeta";

    fn transform(
        &mut self,
        data: databend_common_expression::DataBlock,
    ) -> databend_common_exception::Result<Vec<databend_common_expression::DataBlock>> {
        let commit_meta = CommitMeta::try_from(data)?;
        self.to_merged.push(commit_meta);
        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        let to_merged = std::mem::take(&mut self.to_merged);
        if to_merged.is_empty() {
            return Ok(vec![]);
        }
        let table_id = to_merged[0].table_id;
        let merged = to_merged
            .into_iter()
            .fold(CommitMeta::empty(table_id), |acc, x| {
                Self::merge_commit_meta(acc, x, self.default_cluster_key_id)
            });
        Ok(vec![merged.into()])
    }
}
