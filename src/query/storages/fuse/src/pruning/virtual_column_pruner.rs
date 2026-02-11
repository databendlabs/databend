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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::VirtualColumnField;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::VirtualColumnIdWithMeta;
use databend_storages_common_index::VirtualColumnNameIndex;
use databend_storages_common_index::VirtualColumnNode;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_pruner::VirtualColumnReadPlan;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use jsonb::keypath::OwnedKeyPath;
use jsonb::keypath::OwnedKeyPaths;
use opendal::Operator;

use crate::io::TableMetaLocationGenerator;
use crate::io::read::load_virtual_column_file_meta;

pub struct VirtualColumnPruner {
    dal: Operator,
    source_column_ids: HashSet<ColumnId>,
    // Cache path matching info once to avoid repeated parsing per block.
    virtual_column_fields: Vec<VirtualColumnFieldMatch>,
}

struct VirtualColumnFieldMatch {
    field: VirtualColumnField,
    match_info: KeyPathMatchInfo,
}

impl VirtualColumnPruner {
    pub fn try_create(
        dal: Operator,
        push_down: &Option<PushDownInfo>,
    ) -> Result<Option<Arc<VirtualColumnPruner>>> {
        let virtual_column = push_down.as_ref().and_then(|p| p.virtual_column.as_ref());
        if let Some(virtual_column) = virtual_column {
            let mut virtual_column_fields =
                Vec::with_capacity(virtual_column.virtual_column_fields.len());
            for field in &virtual_column.virtual_column_fields {
                let match_info = key_paths_match_info(&field.key_paths);
                virtual_column_fields.push(VirtualColumnFieldMatch {
                    field: field.clone(),
                    match_info,
                });
            }
            return Ok(Some(Arc::new(VirtualColumnPruner {
                dal,
                source_column_ids: virtual_column.source_column_ids.clone(),
                virtual_column_fields,
            })));
        }
        Ok(None)
    }

    #[async_backtrace::framed]
    pub async fn prune_virtual_columns(
        &self,
        virtual_block_meta: &Option<VirtualBlockMeta>,
    ) -> Result<Option<VirtualBlockMetaIndex>> {
        let Some(virtual_block_meta) = virtual_block_meta else {
            return Ok(None);
        };
        if virtual_block_meta.virtual_column_size == 0 {
            return Ok(None);
        }
        if TableMetaLocationGenerator::is_legacy_virtual_block_location(
            &virtual_block_meta.virtual_location.0,
        ) {
            return Ok(None);
        }

        let Ok(virtual_meta) =
            load_virtual_column_file_meta(self.dal.clone(), &virtual_block_meta.virtual_location.0)
                .await
        else {
            return Ok(None);
        };

        // Query plan model:
        // - Direct: exact path is a materialized virtual column.
        // - Shared: sparse path stored in the shared map column.
        // - Object: reconstruct parent object from child plans.
        // - FromParent: read nearest variant parent and extract suffix via keypath.
        let mut virtual_column_metas = BTreeMap::new();
        // Each column can have multiple read plans due to heterogeneous JSON shapes.
        let mut virtual_column_read_plan = BTreeMap::new();
        let mut shared_virtual_column_ids = BTreeMap::new();
        let mut need_source_column_ids = HashSet::new();

        let string_table_index: HashMap<String, u32> = virtual_meta
            .string_table
            .iter()
            .enumerate()
            .map(|(id, name)| (name.clone(), id as u32))
            .collect();

        for virtual_column_field in &self.virtual_column_fields {
            let mut plans = Vec::new();
            let field = &virtual_column_field.field;
            let source_column_id = field.source_column_id;
            let match_info = &virtual_column_field.match_info;
            let segments = &match_info.segments;
            let name_positions = &match_info.name_positions;
            let has_index = match_info.has_index;
            let mut segment_ids = Vec::with_capacity(segments.len());
            let mut all_segments_known = true;
            for segment in segments {
                let Some(segment_id) = string_table_index.get(segment) else {
                    all_segments_known = false;
                    break;
                };
                segment_ids.push(*segment_id);
            }

            let mut matched_node: Option<&VirtualColumnNode> = None;
            let mut last_variant_leaf: Option<usize> = None;
            let mut prefix_nodes: Vec<&VirtualColumnNode> = Vec::new();
            if let Some(root) = virtual_meta.virtual_column_nodes.get(&source_column_id) {
                let mut node = root;
                let mut prefix_len = 0;
                let mut failed = false;
                for segment_id in &segment_ids {
                    let Some(child) = node.children.get(segment_id) else {
                        failed = true;
                        break;
                    };
                    node = child;
                    prefix_nodes.push(node);
                    prefix_len += 1;
                    if let Some(VirtualColumnNameIndex::Column(leaf_index)) = node.leaf.as_ref() {
                        if let Some(meta) = virtual_meta.column_metas.get(*leaf_index as usize) {
                            // Variant columns allow extracting deeper fields via get_by_keypath.
                            if is_variant_meta(meta) {
                                last_variant_leaf = Some(prefix_len);
                            }
                        }
                    }
                }
                if !failed && all_segments_known && segment_ids.len() == segments.len() {
                    matched_node = Some(node);
                }
            }

            if let Some(node) = matched_node {
                if !has_index {
                    // Build direct/object/shared plans when the full path exists in the trie.
                    let mut node_plans = build_plans_for_node(
                        node,
                        source_column_id,
                        segments,
                        &virtual_meta,
                        &mut virtual_column_metas,
                        &mut shared_virtual_column_ids,
                    );
                    plans.append(&mut node_plans);
                }
            }

            if let Some(prefix_len) = last_variant_leaf {
                if prefix_len <= prefix_nodes.len() {
                    // Build FromParent plans when only a prefix exists as a variant column.
                    // The suffix will be extracted at read time.
                    let parent_node = prefix_nodes[prefix_len - 1];
                    let parent_segments = &segments[..prefix_len];
                    let parent_plans = build_plans_for_node(
                        parent_node,
                        source_column_id,
                        parent_segments,
                        &virtual_meta,
                        &mut virtual_column_metas,
                        &mut shared_virtual_column_ids,
                    );
                    let suffix_start = name_positions
                        .get(prefix_len.saturating_sub(1))
                        .copied()
                        .unwrap_or(field.key_paths.paths.len());
                    let suffix_path =
                        build_virtual_column_suffix_path(&field.key_paths, suffix_start);
                    if !suffix_path.is_empty() {
                        for parent_plan in parent_plans {
                            plans.push(VirtualColumnReadPlan::FromParent {
                                parent: Box::new(parent_plan),
                                suffix_path: suffix_path.clone(),
                            });
                        }
                    }
                }
            }

            if !plans.is_empty() {
                let entry = virtual_column_read_plan
                    .entry(field.column_id)
                    .or_insert_with(Vec::new);
                for plan in plans {
                    if !entry.contains(&plan) {
                        entry.push(plan);
                    }
                }
            } else {
                // Fallback: read the original source column when no virtual plan matches.
                // The virtual column does not exist and must be generated from the source column.
                need_source_column_ids.insert(source_column_id);
            }
        }

        // The remaining source column can be ignored.
        let mut ignored_source_column_ids = HashSet::new();
        for column_id in self.source_column_ids.difference(&need_source_column_ids) {
            ignored_source_column_ids.insert(*column_id);
        }

        if !virtual_column_metas.is_empty() {
            let virtual_block_meta = VirtualBlockMetaIndex {
                virtual_block_location: virtual_block_meta.virtual_location.0.clone(),
                virtual_column_metas,
                shared_virtual_column_ids,
                ignored_source_column_ids,
                virtual_column_read_plan,
            };
            return Ok(Some(virtual_block_meta));
        }
        Ok(None)
    }
}

fn virtual_column_data_type_code(data_type: &DataType) -> u8 {
    match data_type.remove_nullable() {
        DataType::Variant => 0,
        DataType::Boolean => 1,
        DataType::Number(NumberDataType::UInt64) => 2,
        DataType::Number(NumberDataType::Int64) => 3,
        DataType::Number(NumberDataType::Float64) => 4,
        DataType::String => 5,
        _ => unreachable!(),
    }
}

fn build_virtual_column_meta(meta: &VirtualColumnIdWithMeta) -> VirtualColumnMeta {
    VirtualColumnMeta {
        offset: meta.meta.offset,
        len: meta.meta.len,
        num_values: meta.meta.num_values,
        data_type: virtual_column_data_type_code(&meta.data_type),
        column_stat: None,
    }
}

fn is_variant_meta(meta: &VirtualColumnIdWithMeta) -> bool {
    meta.data_type.remove_nullable() == DataType::Variant
}

fn ensure_virtual_column_id(
    virtual_column_metas: &mut BTreeMap<ColumnId, VirtualColumnMeta>,
    meta: &VirtualColumnIdWithMeta,
) -> ColumnId {
    let column_id = meta.column_id;
    if !virtual_column_metas.contains_key(&column_id) {
        virtual_column_metas.insert(column_id, build_virtual_column_meta(meta));
    }
    column_id
}

fn ensure_shared_virtual_column_ids(
    virtual_column_metas: &mut BTreeMap<ColumnId, VirtualColumnMeta>,
    shared_virtual_column_ids: &mut BTreeMap<ColumnId, ColumnId>,
    shared_column_metas: &HashMap<u32, (VirtualColumnIdWithMeta, VirtualColumnIdWithMeta)>,
    source_column_id: u32,
) -> bool {
    if shared_virtual_column_ids.contains_key(&source_column_id) {
        return true;
    }
    let Some((key_meta, value_meta)) = shared_column_metas.get(&source_column_id) else {
        return false;
    };
    let key_id = key_meta.column_id;
    let value_id = value_meta.column_id;
    if !virtual_column_metas.contains_key(&key_id) {
        virtual_column_metas.insert(key_id, VirtualColumnMeta {
            offset: key_meta.meta.offset,
            len: key_meta.meta.len,
            num_values: key_meta.meta.num_values,
            data_type: 0,
            column_stat: None,
        });
    }
    if !virtual_column_metas.contains_key(&value_id) {
        virtual_column_metas.insert(value_id, VirtualColumnMeta {
            offset: value_meta.meta.offset,
            len: value_meta.meta.len,
            num_values: value_meta.meta.num_values,
            data_type: 0,
            column_stat: None,
        });
    }
    shared_virtual_column_ids.insert(source_column_id, key_id);
    true
}

fn build_plans_for_node(
    node: &VirtualColumnNode,
    source_column_id: u32,
    segments: &[String],
    virtual_meta: &VirtualColumnFileMeta,
    virtual_column_metas: &mut BTreeMap<ColumnId, VirtualColumnMeta>,
    shared_virtual_column_ids: &mut BTreeMap<ColumnId, ColumnId>,
) -> Vec<VirtualColumnReadPlan> {
    let mut plans = Vec::new();

    if let Some(leaf) = node.leaf.as_ref() {
        match leaf {
            VirtualColumnNameIndex::Column(leaf_index) => {
                if let Some(meta) = virtual_meta.column_metas.get(*leaf_index as usize) {
                    ensure_virtual_column_id(virtual_column_metas, meta);
                    // Direct: read the materialized virtual column.
                    let name = meta.column_id.to_string();
                    plans.push(VirtualColumnReadPlan::Direct { name });
                }
            }
            VirtualColumnNameIndex::Shared(index) => {
                if ensure_shared_virtual_column_ids(
                    virtual_column_metas,
                    shared_virtual_column_ids,
                    &virtual_meta.shared_column_metas,
                    source_column_id,
                ) {
                    // Shared: read from the shared map column by key index.
                    plans.push(VirtualColumnReadPlan::Shared {
                        source_column_id,
                        index: *index,
                    });
                }
            }
        }
    }

    let mut children: Vec<(u32, &VirtualColumnNode)> = node
        .children
        .iter()
        .map(|(id, child)| (*id, child))
        .collect();
    children.sort_by_key(|(id, _)| *id);
    let mut entries = Vec::new();
    for (child_id, child_node) in children {
        let Some(segment_name) = virtual_meta.string_table.get(child_id as usize) else {
            continue;
        };
        let Some(child_key) = segment_to_object_key(segment_name) else {
            continue;
        };
        let mut child_segments = segments.to_vec();
        child_segments.push(segment_name.to_string());
        let child_plans = build_plans_for_node(
            child_node,
            source_column_id,
            &child_segments,
            virtual_meta,
            virtual_column_metas,
            shared_virtual_column_ids,
        );
        if let Some(plan) = child_plans.into_iter().next() {
            entries.push((child_key, plan));
        }
    }
    if !entries.is_empty() {
        // Object: reconstruct a parent object from child plans.
        plans.push(VirtualColumnReadPlan::Object { entries });
    }

    plans
}

struct KeyPathMatchInfo {
    // segments: name-only path segments until the first array index.
    segments: Vec<String>,
    // name_positions: positions of Name/QuotedName in original key paths.
    name_positions: Vec<usize>,
    // has_index: any array index forces extraction from parent instead of trie match.
    has_index: bool,
}

fn key_paths_match_info(key_paths: &OwnedKeyPaths) -> KeyPathMatchInfo {
    let mut segments = Vec::new();
    let mut name_positions = Vec::new();
    let mut has_index = false;
    for (idx, path) in key_paths.paths.iter().enumerate() {
        match path {
            OwnedKeyPath::Index(_) => {
                has_index = true;
            }
            OwnedKeyPath::Name(name) | OwnedKeyPath::QuotedName(name) => {
                if has_index {
                    continue;
                }
                segments.push(name.to_string());
                name_positions.push(idx + 1);
            }
        }
    }
    KeyPathMatchInfo {
        segments,
        name_positions,
        has_index,
    }
}

fn build_virtual_column_suffix_path(key_paths: &OwnedKeyPaths, start: usize) -> String {
    if start >= key_paths.paths.len() {
        return String::new();
    }
    let suffix = OwnedKeyPaths {
        paths: key_paths.paths[start..].to_vec(),
    };
    suffix.to_string()
}

fn segment_to_object_key(segment: &str) -> Option<String> {
    if segment.is_empty() {
        return None;
    }
    Some(segment.to_string())
}
