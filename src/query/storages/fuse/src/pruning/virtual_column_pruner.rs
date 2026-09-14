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
use databend_common_expression::Scalar;
use databend_common_expression::cast_scalar;
use databend_common_expression::format_runtime_keypaths;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::VirtualColumnIdWithMeta;
use databend_storages_common_index::VirtualColumnNameIndex;
use databend_storages_common_index::VirtualColumnNode;
use databend_storages_common_index::VirtualColumnSharedColumnMetaMap;
use databend_storages_common_index::VirtualColumnSharedDataType;
use databend_storages_common_pruner::ProjectedVirtualSegmentSchema;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_pruner::VirtualFieldReadPlan;
use databend_storages_common_pruner::VirtualReadSlot;
use databend_storages_common_pruner::VirtualReadSlotId;
use databend_storages_common_table_meta::meta::ColumnStatistics;
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
    // Query-time column ID of the typed virtual field used by single-column ORDER BY ... LIMIT.
    top_n_query_column_id: Option<ColumnId>,
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
        let Some(push_down) = push_down.as_ref() else {
            return Ok(None);
        };
        let Some(virtual_column) = push_down.virtual_column.as_ref() else {
            return Ok(None);
        };
        let top_n_query_column_id = push_down
            .order_by_virtual_column()
            .map(|field| field.query_column_id);
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
        Ok(Some(Arc::new(VirtualColumnPruner {
            dal,
            source_column_ids: virtual_column.source_column_ids.clone(),
            virtual_column_fields,
            top_n_query_column_id,
        })))
    }

    #[async_backtrace::framed]
    pub async fn prune_virtual_columns(
        &self,
        virtual_block_meta: &Option<VirtualBlockMeta>,
        projected_virtual_schema: Option<&ProjectedVirtualSegmentSchema>,
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

        if let Some(index) =
            self.try_prune_from_block_meta(virtual_block_meta, projected_virtual_schema)
        {
            return Ok(Some(index));
        }

        let Ok(virtual_meta) =
            load_virtual_column_file_meta(self.dal.clone(), &virtual_block_meta.virtual_location.0)
                .await
        else {
            // Read planning can still fall back to the authoritative source column.
            return Ok(None);
        };

        // Query plan model:
        // - Direct: exact path is a materialized virtual column.
        // - Shared: sparse path stored in the shared map column.
        // - Object: reconstruct parent object from child plans.
        // - FromParent: read nearest variant parent and extract suffix via keypath.
        let mut slot_builder = VirtualReadSlotBuilder::default();
        let mut fields = BTreeMap::new();
        let mut fallback_source_column_ids = HashSet::new();
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
                        projected_virtual_schema,
                        &mut slot_builder,
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
                        projected_virtual_schema,
                        &mut slot_builder,
                    )?;
                    let suffix_start = name_positions
                        .get(prefix_len.saturating_sub(1))
                        .copied()
                        .unwrap_or(field.key_paths.paths.len());
                    let suffix_path =
                        build_virtual_column_suffix_path(&field.key_paths, suffix_start);
                    if !suffix_path.is_empty() {
                        for parent_plan in parent_plans {
                            plans.push(VirtualFieldReadPlan::FromParent {
                                parent: Box::new(parent_plan),
                                suffix_path: suffix_path.clone(),
                            });
                        }
                    }
                }
            }

            if let Some(plan) = coalesce_read_plans(plans) {
                fields.insert(field.query_column_id, plan);
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
                fields.insert(field.query_column_id, VirtualFieldReadPlan::Missing);
            }
        }

        // Source columns are ignored only when every requested virtual path from the source has a
        // virtual read plan. Unresolved indexed paths keep the source column for fallback.
        let ignored_source_column_ids = self
            .source_column_ids
            .difference(&fallback_source_column_ids)
            .copied()
            .collect();

        if !fields.is_empty() {
            let virtual_block_meta = VirtualBlockMetaIndex {
                virtual_block_location: virtual_block_meta.virtual_location.0.clone(),
                fields,
                read_slots: slot_builder.into_slots(),
                ignored_source_column_ids,
                virtual_column_stats: HashMap::new(),
            };
            return Ok(Some(virtual_block_meta));
        }
        Ok(None)
    }

    fn try_prune_from_block_meta(
        &self,
        virtual_block_meta: &VirtualBlockMeta,
        projected_virtual_schema: Option<&ProjectedVirtualSegmentSchema>,
    ) -> Option<VirtualBlockMetaIndex> {
        // Incomplete block metadata may omit shared ancestors/descendants from
        // the bounded segment schema. The sidecar footer trie is authoritative.
        if !virtual_block_meta.virtual_columns_complete {
            return None;
        }
        let schema = projected_virtual_schema?;

        let mut slot_builder = VirtualReadSlotBuilder::default();
        let mut fields = BTreeMap::new();
        let mut virtual_column_stats = HashMap::new();
        for virtual_column_field in &self.virtual_column_fields {
            let field = &virtual_column_field.field;
            // Array indexes and unextracted/shared paths still need sidecar parquet meta.
            if virtual_column_field.match_info.has_index {
                return None;
            }
            let projected_field =
                schema.get(field.source_column_id, &virtual_column_field.encoded_path)?;
            // Related paths require plans derived from the authoritative sidecar trie.
            if projected_field.has_related_paths() {
                return None;
            }
            let Some(column_id) = projected_field.column_id else {
                fields.insert(field.query_column_id, VirtualFieldReadPlan::Missing);
                continue;
            };
            match virtual_block_meta.virtual_column_metas.get(&column_id) {
                Some(column_meta) => {
                    let slot = slot_builder.add_segment_column(column_id, column_meta);
                    fields.insert(field.query_column_id, VirtualFieldReadPlan::Direct { slot });

                    if self.top_n_query_column_id == Some(field.query_column_id) {
                        let requested_type = DataType::from(field.data_type.as_ref());
                        let physical_type = DataType::from(&column_meta.data_type());
                        if let Some(stat) = column_meta.column_stat.as_ref().and_then(|stat| {
                            cast_virtual_column_statistics(stat, &physical_type, &requested_type)
                        }) {
                            virtual_column_stats.insert(field.query_column_id, stat);
                        }
                    }
                }
                None => {
                    fields.insert(field.query_column_id, VirtualFieldReadPlan::Missing);
                }
            }
        }

        Some(VirtualBlockMetaIndex {
            virtual_block_location: virtual_block_meta.virtual_location.0.clone(),
            fields,
            read_slots: slot_builder.into_slots(),
            ignored_source_column_ids: self.source_column_ids.clone(),
            virtual_column_stats,
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

/// Convert physical direct-column statistics into the query-visible virtual
/// column type. TopN only needs an interval containing every value after the
/// query cast; the conversion does not have to be lossless or injective.
///
/// Keep this list restricted to monotonic numeric conversions. Strictly casting
/// both endpoints proves that a narrowing integer or decimal conversion succeeds
/// for the entire block. Floating-point bounds are expanded by one ULP to remain
/// conservative around lossy integer/decimal conversions.
fn cast_virtual_column_statistics(
    statistics: &ColumnStatistics,
    physical_type: &DataType,
    requested_type: &DataType,
) -> Option<ColumnStatistics> {
    if physical_type == requested_type {
        return Some(statistics.clone());
    }

    let physical_type = physical_type.remove_nullable();
    let requested_type = requested_type.remove_nullable();
    if !is_monotonic_statistics_cast(&physical_type, &requested_type) {
        return None;
    }

    let statistics_type = statistics
        .min
        .as_ref()
        .infer_common_type(&statistics.max.as_ref())?;
    if statistics_type.remove_nullable() != physical_type {
        return None;
    }

    let min = cast_scalar(
        None,
        statistics.min.clone(),
        &requested_type,
        &BUILTIN_FUNCTIONS,
    )
    .ok()?;
    let max = cast_scalar(
        None,
        statistics.max.clone(),
        &requested_type,
        &BUILTIN_FUNCTIONS,
    )
    .ok()?;
    if min.is_null() || max.is_null() || min > max {
        return None;
    }

    let (min, max) = expand_float_statistics_bounds(min, max)?;
    Some(ColumnStatistics::new(
        min,
        max,
        statistics.null_count,
        statistics.in_memory_size,
        None,
    ))
}

fn is_monotonic_statistics_cast(physical_type: &DataType, requested_type: &DataType) -> bool {
    match (physical_type, requested_type) {
        (DataType::Number(src), DataType::Number(dest)) => {
            src.is_integer()
                || (*src == NumberDataType::Float32 && *dest == NumberDataType::Float64)
        }
        (DataType::Number(src), DataType::Decimal(_)) => src.is_integer(),
        (DataType::Decimal(_), DataType::Decimal(_)) => true,
        (DataType::Decimal(_), DataType::Number(dest)) => dest.is_float(),
        _ => false,
    }
}

fn expand_float_statistics_bounds(min: Scalar, max: Scalar) -> Option<(Scalar, Scalar)> {
    match (min, max) {
        (
            Scalar::Number(NumberScalar::Float32(min)),
            Scalar::Number(NumberScalar::Float32(max)),
        ) if min.is_finite() && max.is_finite() => Some((
            Scalar::Number(NumberScalar::Float32(min.next_down())),
            Scalar::Number(NumberScalar::Float32(max.next_up())),
        )),
        (
            Scalar::Number(NumberScalar::Float64(min)),
            Scalar::Number(NumberScalar::Float64(max)),
        ) if min.is_finite() && max.is_finite() => Some((
            Scalar::Number(NumberScalar::Float64(min.next_down())),
            Scalar::Number(NumberScalar::Float64(max.next_up())),
        )),
        (Scalar::Number(NumberScalar::Float32(_)), Scalar::Number(NumberScalar::Float32(_)))
        | (Scalar::Number(NumberScalar::Float64(_)), Scalar::Number(NumberScalar::Float64(_))) => {
            None
        }
        (min, max) => Some((min, max)),
    }
}

fn direct_virtual_column_meta(
    source_column_id: ColumnId,
    canonical_path: &str,
    footer_meta: &VirtualColumnIdWithMeta,
    block_meta: &VirtualBlockMeta,
    projected_virtual_schema: Option<&ProjectedVirtualSegmentSchema>,
) -> Result<VirtualColumnMeta> {
    // Requested paths can reuse compact block metadata through their segment-local ID.
    // Recursive object/parent plans may visit non-requested paths omitted from the
    // projection; for those paths the Parquet footer remains authoritative and supplies
    // the actual ordinal, byte range, and physical type.
    let block_column_meta = projected_virtual_schema
        .and_then(|schema| schema.find_column_id(source_column_id, canonical_path))
        .and_then(|column_id| block_meta.virtual_column_metas.get(&column_id));
    match block_column_meta {
        Some(meta) => Ok(meta.clone()),
        None => footer_meta.to_virtual_column_meta(),
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum PhysicalSlotKey {
    SegmentColumn(ColumnId),
    ParquetLeaf(u32),
}

#[derive(Default)]
struct VirtualReadSlotBuilder {
    slots: Vec<VirtualReadSlot>,
    slots_by_physical_column: HashMap<PhysicalSlotKey, VirtualReadSlotId>,
    shared_slots:
        HashMap<(ColumnId, VirtualColumnSharedDataType), (VirtualReadSlotId, VirtualReadSlotId)>,
}

impl VirtualReadSlotBuilder {
    fn into_slots(self) -> Vec<VirtualReadSlot> {
        self.slots
    }

    fn add_segment_column(
        &mut self,
        column_id: ColumnId,
        meta: &VirtualColumnMeta,
    ) -> VirtualReadSlotId {
        let key = PhysicalSlotKey::SegmentColumn(column_id);
        if let Some(slot) = self.slots_by_physical_column.get(&key) {
            return *slot;
        }
        let slot = self.push_slot(VirtualReadSlot {
            offset: meta.offset,
            len: meta.len,
            num_values: meta.num_values,
            data_type: DataType::from(&meta.data_type()),
        });
        self.slots_by_physical_column.insert(key, slot);
        slot
    }

    fn add_parquet_column(
        &mut self,
        meta: &VirtualColumnIdWithMeta,
        column_meta: &VirtualColumnMeta,
    ) -> VirtualReadSlotId {
        let key = PhysicalSlotKey::ParquetLeaf(meta.parquet_column_id);
        if let Some(slot) = self.slots_by_physical_column.get(&key) {
            return *slot;
        }
        let slot = self.push_slot(VirtualReadSlot {
            offset: column_meta.offset,
            len: column_meta.len,
            num_values: column_meta.num_values,
            data_type: DataType::from(&column_meta.data_type()),
        });
        self.slots_by_physical_column.insert(key, slot);
        slot
    }

    fn add_shared_columns(
        &mut self,
        typed_shared_column_metas: &VirtualColumnSharedColumnMetaMap,
        source_column_id: ColumnId,
        data_type: VirtualColumnSharedDataType,
    ) -> Option<(VirtualReadSlotId, VirtualReadSlotId)> {
        let shared_key = (source_column_id, data_type);
        if let Some(slots) = self.shared_slots.get(&shared_key) {
            return Some(*slots);
        }
        let source_shared_metas = typed_shared_column_metas.get(&source_column_id)?;
        let (key_meta, value_meta) = source_shared_metas.get(&data_type)?;

        // Shared key/value slots are deliberately allocated together. The
        // Parquet map decoder consumes them as one adjacent logical column.
        let key_slot = self.push_slot(VirtualReadSlot {
            offset: key_meta.meta.offset,
            len: key_meta.meta.len,
            num_values: key_meta.meta.num_values,
            data_type: key_meta.data_type.clone(),
        });
        let value_slot = self.push_slot(VirtualReadSlot {
            offset: value_meta.meta.offset,
            len: value_meta.meta.len,
            num_values: value_meta.meta.num_values,
            data_type: value_meta.data_type.clone(),
        });
        debug_assert_eq!(value_slot.0, key_slot.0 + 1);
        self.shared_slots.insert(shared_key, (key_slot, value_slot));
        Some((key_slot, value_slot))
    }

    fn push_slot(&mut self, slot: VirtualReadSlot) -> VirtualReadSlotId {
        let id = VirtualReadSlotId(self.slots.len() as u32);
        self.slots.push(slot);
        id
    }
}

fn build_plans_for_node(
    node: &VirtualColumnNode,
    source_column_id: ColumnId,
    segments: &[String],
    virtual_meta: &VirtualColumnFileMeta,
    block_meta: &VirtualBlockMeta,
    projected_virtual_schema: Option<&ProjectedVirtualSegmentSchema>,
    slot_builder: &mut VirtualReadSlotBuilder,
) -> Result<Vec<VirtualFieldReadPlan>> {
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
                    projected_virtual_schema,
                )?;
                let slot = slot_builder.add_parquet_column(meta, &column_meta);
                plans.push(VirtualFieldReadPlan::Direct { slot });
            }
            VirtualColumnNameIndex::Shared(index) => {
                if let Some((key_slot, value_slot)) = slot_builder.add_shared_columns(
                    &virtual_meta.typed_shared_column_metas,
                    source_column_id,
                    VirtualColumnSharedDataType::Jsonb,
                ) {
                    plans.push(VirtualFieldReadPlan::Shared {
                        key_slot,
                        value_slot,
                        index: *index,
                    });
                }
            }
            VirtualColumnNameIndex::TypedShared { data_type, index } => {
                if let Some((key_slot, value_slot)) = slot_builder.add_shared_columns(
                    &virtual_meta.typed_shared_column_metas,
                    source_column_id,
                    *data_type,
                ) {
                    plans.push(VirtualFieldReadPlan::Shared {
                        key_slot,
                        value_slot,
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
            projected_virtual_schema,
            slot_builder,
        )?;
        if let Some(plan) = coalesce_read_plans(child_plans) {
            entries.push((child_key, plan));
        }
    }
    if !entries.is_empty() {
        plans.push(VirtualFieldReadPlan::Object { entries });
    }

    Ok(plans)
}

fn coalesce_read_plans(plans: Vec<VirtualFieldReadPlan>) -> Option<VirtualFieldReadPlan> {
    let mut unique = Vec::with_capacity(plans.len());
    for plan in plans {
        if !unique.contains(&plan) {
            unique.push(plan);
        }
    }
    let mut plans = unique;
    match plans.len() {
        0 => None,
        1 => plans.pop(),
        _ => Some(VirtualFieldReadPlan::Coalesce { plans }),
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
    format_runtime_keypaths(&suffix)
}

fn segment_to_object_key(segment: &str) -> Option<String> {
    if segment.is_empty() {
        return None;
    }
    Some(segment.to_string())
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::number::F64;

    use super::*;

    #[test]
    fn test_cast_virtual_column_statistics() {
        let uint64_type = DataType::Number(NumberDataType::UInt64);
        let int64_type = DataType::Number(NumberDataType::Int64);
        let statistics = ColumnStatistics::new(
            Scalar::Number(NumberScalar::UInt64(10)),
            Scalar::Number(NumberScalar::UInt64(20)),
            1,
            16,
            Some(11),
        );
        let converted =
            cast_virtual_column_statistics(&statistics, &uint64_type, &int64_type).unwrap();
        assert_eq!(converted.min(), &Scalar::Number(NumberScalar::Int64(10)));
        assert_eq!(converted.max(), &Scalar::Number(NumberScalar::Int64(20)));
        assert_eq!(converted.null_count, 1);
        assert_eq!(converted.in_memory_size, 16);
        assert_eq!(converted.distinct_of_values, None);

        let overflowing = ColumnStatistics::new(
            Scalar::Number(NumberScalar::UInt64(i64::MAX as u64)),
            Scalar::Number(NumberScalar::UInt64(i64::MAX as u64 + 1)),
            0,
            16,
            None,
        );
        assert!(cast_virtual_column_statistics(&overflowing, &uint64_type, &int64_type).is_none());

        let decimal_size = DecimalSize::new(18, 1).unwrap();
        let decimal_type = DataType::Decimal(decimal_size);
        let float64_type = DataType::Number(NumberDataType::Float64);
        let decimal_statistics = ColumnStatistics::new(
            Scalar::Decimal(DecimalScalar::Decimal64(955, decimal_size)),
            Scalar::Decimal(DecimalScalar::Decimal64(1005, decimal_size)),
            0,
            16,
            None,
        );
        let converted =
            cast_virtual_column_statistics(&decimal_statistics, &decimal_type, &float64_type)
                .unwrap();
        assert_eq!(
            converted.min(),
            &Scalar::Number(NumberScalar::Float64(F64::from(95.5_f64.next_down())))
        );
        assert_eq!(
            converted.max(),
            &Scalar::Number(NumberScalar::Float64(F64::from(100.5_f64.next_up())))
        );
    }
}
