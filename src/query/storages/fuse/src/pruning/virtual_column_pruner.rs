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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::types::DataType;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::VirtualColumnIdWithMeta;
use databend_storages_common_index::VirtualColumnNameIndex;
use databend_storages_common_index::VirtualColumnNode;
use databend_storages_common_index::VirtualColumnSharedColumnMetaMap;
use databend_storages_common_index::VirtualColumnSharedDataType;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_pruner::VirtualColumnReadPlan;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;
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
    encoded_path: String,
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
                    encoded_path: field.key_paths.to_canonical_path(),
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
        virtual_segment_schema: Option<&VirtualSegmentSchema>,
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

        let virtual_column_stats = build_runtime_virtual_column_stats(
            virtual_block_meta,
            virtual_segment_schema,
            &self.virtual_column_fields,
        );
        if let Some(mut index) =
            self.try_prune_from_block_meta(virtual_block_meta, virtual_segment_schema)
        {
            index.virtual_column_stats = virtual_column_stats;
            return Ok(Some(index));
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
        let mut fallback_source_column_ids = HashSet::new();
        let mut shared_virtual_column_ids = BTreeMap::new();
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
            let starts_with_index = match_info.starts_with_index;
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
            let mut last_jsonb_parent: Option<usize> = None;
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
                    if node_has_jsonb_parent_plan(node, &virtual_meta) {
                        last_jsonb_parent = Some(prefix_len);
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
                        virtual_block_meta,
                        virtual_segment_schema,
                        &mut virtual_column_metas,
                        &mut shared_virtual_column_ids,
                    )?;
                    plans.append(&mut node_plans);
                }
            }

            if let Some(prefix_len) = last_jsonb_parent {
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
                        virtual_block_meta,
                        virtual_segment_schema,
                        &mut virtual_column_metas,
                        &mut shared_virtual_column_ids,
                    )?;
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
                    .entry(field.query_column_id)
                    .or_insert_with(Vec::new);
                for plan in plans {
                    if !entry.contains(&plan) {
                        entry.push(plan);
                    }
                }
            } else if starts_with_index {
                // Virtual column metadata currently does not fully support unresolved array-index
                // paths. In particular, root-array paths like `v[0]['k']` have no object segment
                // before the first index, so there is no materialized parent in the virtual file
                // to extract from. Keep the source column so the reader can fall back to
                // `get_by_keypath`. A future format can support this by materializing a root
                // JSONB parent or indexed virtual paths.
                fallback_source_column_ids.insert(source_column_id);
            } else {
                // The virtual column file was generated from the observed paths in this block.
                // If a requested path is absent from the trie/shared metadata, the path is absent
                // for this block and can be materialized as NULL without reading the source column.
                virtual_column_read_plan
                    .entry(field.query_column_id)
                    .or_insert_with(Vec::new)
                    .push(VirtualColumnReadPlan::Missing);
            }
        }

        // Source columns are ignored only when every requested virtual path from the source has a
        // virtual read plan. Unresolved indexed paths keep the source column for fallback.
        let ignored_source_column_ids = self
            .source_column_ids
            .difference(&fallback_source_column_ids)
            .copied()
            .collect();

        if !virtual_column_read_plan.is_empty() {
            let virtual_block_meta = VirtualBlockMetaIndex {
                virtual_block_location: virtual_block_meta.virtual_location.0.clone(),
                virtual_column_stats,
                virtual_column_metas,
                shared_virtual_column_ids,
                ignored_source_column_ids,
                virtual_column_read_plan,
            };
            return Ok(Some(virtual_block_meta));
        }
        Ok(None)
    }

    fn try_prune_from_block_meta(
        &self,
        virtual_block_meta: &VirtualBlockMeta,
        virtual_segment_schema: Option<&VirtualSegmentSchema>,
    ) -> Option<VirtualBlockMetaIndex> {
        let schema = virtual_segment_schema?;

        let mut virtual_column_metas = BTreeMap::new();
        let mut virtual_column_read_plan = BTreeMap::new();
        for virtual_column_field in &self.virtual_column_fields {
            let field = &virtual_column_field.field;
            // Array indexes and unextracted/shared paths still need sidecar parquet meta.
            if virtual_column_field.match_info.has_index {
                return None;
            }
            let Some(path) =
                schema.find_path_ref(field.source_column_id, &virtual_column_field.encoded_path)
            else {
                if virtual_block_meta.virtual_columns_complete {
                    virtual_column_read_plan
                        .insert(field.query_column_id, vec![VirtualColumnReadPlan::Missing]);
                    continue;
                }
                return None;
            };
            // A parent path may coexist with materialized descendant paths, for example
            // `geo` and `geo.lat` when some rows store a scalar and others an object.
            // Reading such paths correctly needs Object/Coalesce read plans derived from
            // the sidecar trie, which BlockMeta-only pruning cannot build yet.
            if schema
                .has_descendant_paths(field.source_column_id, &virtual_column_field.encoded_path)
            {
                return None;
            }
            let column_id = path.column_id;
            match virtual_block_meta.virtual_column_metas.get(&column_id) {
                Some(column_meta) => {
                    virtual_column_metas.insert(column_id, column_meta.clone());
                    virtual_column_read_plan.insert(field.query_column_id, vec![
                        VirtualColumnReadPlan::BlockMetaDirect { column_id },
                    ]);
                }
                None if virtual_block_meta.virtual_columns_complete => {
                    virtual_column_read_plan
                        .insert(field.query_column_id, vec![VirtualColumnReadPlan::Missing]);
                }
                None => return None,
            }
        }

        Some(VirtualBlockMetaIndex {
            virtual_block_location: virtual_block_meta.virtual_location.0.clone(),
            virtual_column_stats: HashMap::new(),
            virtual_column_metas,
            shared_virtual_column_ids: BTreeMap::new(),
            ignored_source_column_ids: self.source_column_ids.clone(),
            virtual_column_read_plan,
        })
    }
}

fn is_variant_meta(meta: &VirtualColumnIdWithMeta) -> bool {
    meta.data_type.remove_nullable() == DataType::Variant
}

fn node_has_jsonb_parent_plan(
    node: &VirtualColumnNode,
    virtual_meta: &VirtualColumnFileMeta,
) -> bool {
    match node.leaf.as_ref() {
        Some(VirtualColumnNameIndex::Column(leaf_index)) => virtual_meta
            .column_metas
            .get(*leaf_index as usize)
            .is_some_and(is_variant_meta),
        Some(VirtualColumnNameIndex::Shared(_)) => true,
        Some(VirtualColumnNameIndex::TypedShared { data_type, .. }) => {
            *data_type == VirtualColumnSharedDataType::Jsonb
        }
        None => false,
    }
}

fn build_runtime_virtual_column_stats(
    block_meta: &VirtualBlockMeta,
    segment_schema: Option<&VirtualSegmentSchema>,
    fields: &[VirtualColumnFieldMatch],
) -> HashMap<ColumnId, ColumnStatistics> {
    let Some(schema) = segment_schema else {
        return HashMap::new();
    };
    fields
        .iter()
        .filter_map(|field| {
            let path = schema.find_path_ref(field.field.source_column_id, &field.encoded_path)?;
            let stat = block_meta
                .virtual_column_metas
                .get(&path.column_id)?
                .column_stat
                .clone()?;
            Some((field.field.query_column_id, stat))
        })
        .collect()
}

fn direct_virtual_column_meta(
    source_column_id: ColumnId,
    canonical_path: &str,
    footer_meta: &VirtualColumnIdWithMeta,
    block_meta: &VirtualBlockMeta,
    segment_schema: Option<&VirtualSegmentSchema>,
) -> Result<VirtualColumnMeta> {
    let block_column_meta = segment_schema
        .and_then(|schema| schema.find_path_ref(source_column_id, canonical_path))
        .map(|path| path.column_id)
        .and_then(|column_id| block_meta.virtual_column_metas.get(&column_id));
    match block_column_meta {
        Some(meta) => Ok(meta.clone()),
        None => footer_meta.to_virtual_column_meta(),
    }
}

fn ensure_virtual_column_id(
    virtual_column_metas: &mut BTreeMap<ColumnId, VirtualColumnMeta>,
    meta: &VirtualColumnIdWithMeta,
    column_meta: VirtualColumnMeta,
) -> ColumnId {
    let parquet_column_id = meta.parquet_column_id;
    virtual_column_metas
        .entry(parquet_column_id)
        .or_insert(column_meta);
    parquet_column_id
}

fn ensure_shared_virtual_column_ids(
    virtual_column_metas: &mut BTreeMap<ColumnId, VirtualColumnMeta>,
    shared_virtual_column_ids: &mut BTreeMap<(ColumnId, VirtualColumnSharedDataType), ColumnId>,
    typed_shared_column_metas: &VirtualColumnSharedColumnMetaMap,
    source_column_id: u32,
    data_type: VirtualColumnSharedDataType,
) -> bool {
    if shared_virtual_column_ids.contains_key(&(source_column_id, data_type)) {
        return true;
    }
    let Some(source_shared_metas) = typed_shared_column_metas.get(&source_column_id) else {
        return false;
    };
    let Some((key_meta, value_meta)) = source_shared_metas.get(&data_type) else {
        return false;
    };
    let key_id = key_meta.parquet_column_id;
    let value_id = value_meta.parquet_column_id;
    if !virtual_column_metas.contains_key(&key_id) {
        virtual_column_metas.insert(key_id, VirtualColumnMeta {
            offset: key_meta.meta.offset,
            len: key_meta.meta.len,
            num_values: key_meta.meta.num_values,
            data_type: 0,
            extended_physical_type: None,
            column_stat: None,
        });
    }
    if !virtual_column_metas.contains_key(&value_id) {
        virtual_column_metas.insert(value_id, VirtualColumnMeta {
            offset: value_meta.meta.offset,
            len: value_meta.meta.len,
            num_values: value_meta.meta.num_values,
            data_type: 0,
            extended_physical_type: None,
            column_stat: None,
        });
    }
    shared_virtual_column_ids.insert((source_column_id, data_type), key_id);
    true
}

fn build_plans_for_node(
    node: &VirtualColumnNode,
    source_column_id: u32,
    segments: &[String],
    virtual_meta: &VirtualColumnFileMeta,
    block_meta: &VirtualBlockMeta,
    segment_schema: Option<&VirtualSegmentSchema>,
    virtual_column_metas: &mut BTreeMap<ColumnId, VirtualColumnMeta>,
    shared_virtual_column_ids: &mut BTreeMap<(ColumnId, VirtualColumnSharedDataType), ColumnId>,
) -> Result<Vec<VirtualColumnReadPlan>> {
    let mut plans = Vec::new();

    if let Some(leaf) = node.leaf.as_ref() {
        match leaf {
            VirtualColumnNameIndex::Column(leaf_index) => {
                let meta = virtual_meta
                    .column_metas
                    .get(*leaf_index as usize)
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "virtual column trie references missing parquet column {}",
                            leaf_index
                        ))
                    })?;
                let canonical_path = OwnedKeyPaths {
                    paths: segments.iter().cloned().map(OwnedKeyPath::Name).collect(),
                }
                .to_canonical_path();
                let column_meta = direct_virtual_column_meta(
                    source_column_id,
                    &canonical_path,
                    meta,
                    block_meta,
                    segment_schema,
                )?;
                ensure_virtual_column_id(virtual_column_metas, meta, column_meta);
                // Direct: read the materialized virtual column by parquet ordinal.
                let name = meta.parquet_column_id.to_string();
                plans.push(VirtualColumnReadPlan::Direct { name });
            }
            VirtualColumnNameIndex::Shared(index) => {
                if ensure_shared_virtual_column_ids(
                    virtual_column_metas,
                    shared_virtual_column_ids,
                    &virtual_meta.typed_shared_column_metas,
                    source_column_id,
                    VirtualColumnSharedDataType::Jsonb,
                ) {
                    // Shared: read from the shared map column by key index.
                    plans.push(VirtualColumnReadPlan::Shared {
                        source_column_id,
                        data_type: VirtualColumnSharedDataType::Jsonb,
                        index: *index,
                    });
                }
            }
            VirtualColumnNameIndex::TypedShared { data_type, index } => {
                if ensure_shared_virtual_column_ids(
                    virtual_column_metas,
                    shared_virtual_column_ids,
                    &virtual_meta.typed_shared_column_metas,
                    source_column_id,
                    *data_type,
                ) {
                    // Shared: read from the typed shared map column by key index.
                    plans.push(VirtualColumnReadPlan::Shared {
                        source_column_id,
                        data_type: *data_type,
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
            block_meta,
            segment_schema,
            virtual_column_metas,
            shared_virtual_column_ids,
        )?;
        if let Some(plan) = coalesce_read_plans(child_plans) {
            entries.push((child_key, plan));
        }
    }
    if !entries.is_empty() {
        // Object: reconstruct a parent object from child plans.
        plans.push(VirtualColumnReadPlan::Object { entries });
    }

    Ok(plans)
}

fn coalesce_read_plans(mut plans: Vec<VirtualColumnReadPlan>) -> Option<VirtualColumnReadPlan> {
    match plans.len() {
        0 => None,
        1 => plans.pop(),
        _ => Some(VirtualColumnReadPlan::Coalesce { plans }),
    }
}

struct KeyPathMatchInfo {
    // segments: name-only path segments until the first array index.
    segments: Vec<String>,
    // name_positions: positions of Name/QuotedName in original key paths.
    name_positions: Vec<usize>,
    // has_index: any array index forces extraction from parent instead of trie match.
    has_index: bool,
    // starts_with_index: root-array paths currently need source fallback when unresolved.
    starts_with_index: bool,
}

fn key_paths_match_info(key_paths: &OwnedKeyPaths) -> KeyPathMatchInfo {
    let mut segments = Vec::new();
    let mut name_positions = Vec::new();
    let mut has_index = false;
    let starts_with_index = matches!(key_paths.paths.first(), Some(OwnedKeyPath::Index(_)));
    for (idx, path) in key_paths.paths.iter().enumerate() {
        match path {
            OwnedKeyPath::Index(_) => {
                has_index = true;
            }
            OwnedKeyPath::Name(name) => {
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
        starts_with_index,
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
