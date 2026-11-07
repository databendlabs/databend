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
use databend_common_expression::format_runtime_keypaths;
use databend_common_expression::types::DataType;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::VirtualColumnIdWithMeta;
use databend_storages_common_index::VirtualColumnNameIndex;
use databend_storages_common_index::VirtualColumnNode;
use databend_storages_common_index::VirtualColumnSharedColumnMetaMap;
use databend_storages_common_index::VirtualColumnSharedDataType;
use databend_storages_common_index::cast_virtual_column_statistics;
use databend_storages_common_pruner::ProjectedVirtualSegmentSchema;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_pruner::VirtualFieldReadPlan;
use databend_storages_common_pruner::VirtualReadSlot;
use databend_storages_common_pruner::VirtualReadSlotId;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use databend_storages_common_table_meta::meta::VirtualOverflowBucketMeta;
use databend_storages_common_table_meta::meta::VirtualValueState;
use jsonb::keypath::OwnedKeyPath;
use jsonb::keypath::OwnedKeyPaths;
use opendal::Operator;

use crate::io::TableMetaLocationGenerator;
use crate::io::read::load_virtual_column_file_meta;

pub struct VirtualColumnPruner {
    dal: Operator,
    // Source columns that the scan does not need for their own values.
    ignorable_source_column_ids: HashSet<ColumnId>,
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
            ignorable_source_column_ids: virtual_column.ignorable_source_column_ids.clone(),
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
        // - FromParent: read nearest Variant/Array parent and extract suffix via keypath.
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
            if has_index
                && let Some(root) = virtual_meta.virtual_column_nodes.get(&source_column_id)
                && let Some(plan) = build_array_index_plan(
                    root,
                    source_column_id,
                    &field.key_paths,
                    &virtual_meta,
                    virtual_block_meta,
                    projected_virtual_schema,
                    &mut slot_builder,
                )?
            {
                fields.insert(field.query_column_id, plan);
                continue;
            }
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
            let mut last_extractable_parent: Option<usize> = None;
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
                    if node_has_extractable_parent_plan(node, &virtual_meta) {
                        last_extractable_parent = Some(prefix_len);
                    }
                }
                if !failed && all_segments_known && segment_ids.len() == segments.len() {
                    matched_node = Some(node);
                }
            }

            if let Some(node) = matched_node {
                if !has_index {
                    // Build direct/object/shared plans when the full path exists in the trie.
                    let requested_type = DataType::from(field.data_type.as_ref());
                    let mut node_plans = build_plans_for_node(
                        node,
                        source_column_id,
                        segments,
                        &virtual_meta,
                        virtual_block_meta,
                        projected_virtual_schema,
                        &mut slot_builder,
                        OverflowRead::For(&requested_type),
                    )?;
                    plans.append(&mut node_plans);
                }
            }

            if let Some(prefix_len) = last_extractable_parent {
                if prefix_len <= prefix_nodes.len() {
                    // Extract from the complete Jsonb/typed Array parent at read
                    // time. JSON null, `{}` and `[]` in the parent's overflow
                    // produce NULL for a non-empty suffix, so no overflow read
                    // is needed.
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
                        OverflowRead::Skip,
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
                // Root-array paths like `v[0]['k']` have no object segment before the
                // first index. They are read from the root value column, which stores
                // root scalars and arrays. Blocks without any root column cannot tell
                // whether the root was recorded, so they keep the source column for
                // `get_by_keypath`.
                match build_root_index_plan(
                    source_column_id,
                    &field.key_paths,
                    &virtual_meta,
                    virtual_block_meta,
                    projected_virtual_schema,
                    &mut slot_builder,
                )? {
                    Some(plan) => {
                        fields.insert(field.query_column_id, plan);
                    }
                    None => {
                        fallback_source_column_ids.insert(source_column_id);
                    }
                }
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
            .ignorable_source_column_ids
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
            let block_column = |column_id: Option<ColumnId>| {
                let column_id = column_id?;
                Some((
                    column_id,
                    virtual_block_meta.virtual_column_metas.get(&column_id)?,
                ))
            };
            let requested_type = DataType::from(field.data_type.as_ref());
            let value_meta = block_column(projected_field.column_id);
            let value_plan = value_meta.map(|(column_id, column_meta)| {
                let slot = slot_builder.add_segment_column(column_id, column_meta);
                VirtualFieldReadPlan::Direct { slot }
            });
            let plan = with_overflow(
                value_plan,
                value_meta.map(|(_, meta)| meta.value_state_mask),
                field.source_column_id,
                &virtual_column_field.encoded_path,
                virtual_block_meta,
                OverflowRead::For(&requested_type),
                &mut slot_builder,
            )
            .unwrap_or(VirtualFieldReadPlan::Missing);

            // Value statistics describe the query values only when no overflow is merged.
            if let (VirtualFieldReadPlan::Direct { .. }, Some((_, column_meta))) =
                (&plan, value_meta)
                && self.top_n_query_column_id == Some(field.query_column_id)
            {
                let physical_type = DataType::from(&column_meta.data_type());
                if let Some(stat) = column_meta.column_stat.as_ref().and_then(|stat| {
                    cast_virtual_column_statistics(stat, &physical_type, &requested_type)
                }) {
                    virtual_column_stats.insert(field.query_column_id, stat);
                }
            }
            fields.insert(field.query_column_id, plan);
        }

        Some(VirtualBlockMetaIndex {
            virtual_block_location: virtual_block_meta.virtual_location.0.clone(),
            fields,
            read_slots: slot_builder.into_slots(),
            ignored_source_column_ids: self.ignorable_source_column_ids.clone(),
            virtual_column_stats,
        })
    }
}

fn is_extractable_meta(meta: &VirtualColumnIdWithMeta) -> bool {
    matches!(
        meta.data_type.remove_nullable(),
        DataType::Variant | DataType::Array(_)
    )
}

/// How a plan reads the overflow map of a path.
#[derive(Clone, Copy)]
enum OverflowRead<'a> {
    /// The overflow values of the path do not affect the result.
    Skip,
    /// The path is read as the given type; see `can_skip_overflow`.
    For(&'a DataType),
    /// The path is read as Variant, e.g. to reconstruct an object.
    Always,
    /// Element-space columns are inline list columns, without overflow maps.
    Element,
}

/// Whether the overflow values of a path can be skipped when the path is read
/// as `requested_type`: every value kind in `value_state_mask` must read as
/// NULL for that type, so the value column alone gives the same result.
///
/// Only JSON null qualifies, and only for targets where casting a JSON null
/// yields NULL. `{}` and `[]` must be read, because a strict cast of them
/// fails. String and Variant targets keep the JSON null.
fn can_skip_overflow(value_state_mask: u32, requested_type: &DataType) -> bool {
    let null_only = value_state_mask & !VirtualValueState::JsonNull.mask_bit() == 0;
    null_only
        && matches!(
            requested_type.remove_nullable(),
            DataType::Boolean | DataType::Number(_)
        )
}

/// Merges the overflow map into the plan of a path when the block may store
/// values of the path there.
///
/// `value_state_mask` is the mask of the path's value column when known from
/// BlockMeta; otherwise the mask of the path's overflow bucket is used, which
/// covers every path in the bucket. Returns `value_plan` unchanged when the
/// overflow map is not needed, including for legacy blocks without
/// per-source metadata.
fn with_overflow(
    value_plan: Option<VirtualFieldReadPlan>,
    value_state_mask: Option<u32>,
    source_column_id: ColumnId,
    canonical_path: &str,
    block_meta: &VirtualBlockMeta,
    read: OverflowRead,
    slot_builder: &mut VirtualReadSlotBuilder,
) -> Option<VirtualFieldReadPlan> {
    if matches!(read, OverflowRead::Skip | OverflowRead::Element) {
        return value_plan;
    }
    let Some((bucket, bucket_meta)) = block_meta
        .sources
        .get(&source_column_id)
        .and_then(|source| source.overflow_bucket(canonical_path))
    else {
        return value_plan;
    };
    let mask = value_state_mask.unwrap_or(bucket_meta.value_state_mask);
    if mask == 0 {
        return value_plan;
    }
    if let OverflowRead::For(requested_type) = read
        && can_skip_overflow(mask, requested_type)
    {
        return value_plan;
    }
    let (key_slot, value_slot) =
        slot_builder.add_overflow_bucket(source_column_id, bucket, bucket_meta);
    Some(VirtualFieldReadPlan::WithOverflow {
        value: value_plan.map(Box::new),
        key_slot,
        value_slot,
        path: canonical_path.to_string(),
    })
}

/// The overflow mask of a path's value column from BlockMeta, or `None` when
/// the path is not in the projected schema.
fn block_value_state_mask(
    source_column_id: ColumnId,
    canonical_path: &str,
    block_meta: &VirtualBlockMeta,
    projected_virtual_schema: Option<&ProjectedVirtualSegmentSchema>,
) -> Option<u32> {
    projected_virtual_schema
        .and_then(|schema| schema.find_column_id(source_column_id, canonical_path))
        .and_then(|column_id| block_meta.virtual_column_metas.get(&column_id))
        .map(|meta| meta.value_state_mask)
}

/// Builds the plan of a path that starts with an array index, e.g. `v[0]['k']`.
///
/// Such a path is non-NULL only for rows whose root is a non-empty array, and
/// those are stored in the root value column. Rows whose root is an object, a
/// scalar, SQL NULL, or an overflow value (JSON null, `{}`, `[]`) all read as
/// NULL.
///
/// Returns `None` for legacy blocks, which do not record root values.
fn build_root_index_plan(
    source_column_id: ColumnId,
    key_paths: &OwnedKeyPaths,
    virtual_meta: &VirtualColumnFileMeta,
    block_meta: &VirtualBlockMeta,
    projected_virtual_schema: Option<&ProjectedVirtualSegmentSchema>,
    slot_builder: &mut VirtualReadSlotBuilder,
) -> Result<Option<VirtualFieldReadPlan>> {
    if !block_meta.sources.contains_key(&source_column_id) {
        return Ok(None);
    }
    // Root arrays are either typed scalar Array leaves or whole Jsonb leaves.
    // Object-array groups have already been handled by build_array_index_plan.
    let Some(VirtualColumnNameIndex::Column(leaf_index)) = virtual_meta
        .virtual_column_nodes
        .get(&source_column_id)
        .and_then(|root| root.leaf.as_ref())
    else {
        return Ok(Some(VirtualFieldReadPlan::Missing));
    };
    let meta = virtual_meta
        .column_metas
        .get(*leaf_index as usize)
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "virtual column trie references missing parquet column {}",
                leaf_index
            ))
        })?;
    if !is_extractable_meta(meta) {
        return Ok(Some(VirtualFieldReadPlan::Missing));
    }
    let column_meta = direct_virtual_column_meta(
        source_column_id,
        "",
        meta,
        block_meta,
        projected_virtual_schema,
    )?;
    let slot = slot_builder.add_parquet_column(meta, &column_meta);
    Ok(Some(VirtualFieldReadPlan::FromParent {
        parent: Box::new(VirtualFieldReadPlan::Direct { slot }),
        suffix_path: build_virtual_column_suffix_path(key_paths, 0),
    }))
}

fn node_has_extractable_parent_plan(
    node: &VirtualColumnNode,
    virtual_meta: &VirtualColumnFileMeta,
) -> bool {
    match node.leaf.as_ref() {
        Some(VirtualColumnNameIndex::Column(leaf_index)) => virtual_meta
            .column_metas
            .get(*leaf_index as usize)
            .is_some_and(is_extractable_meta),
        Some(VirtualColumnNameIndex::Shared(_)) => true,
        Some(VirtualColumnNameIndex::TypedShared { data_type, .. }) => {
            *data_type == VirtualColumnSharedDataType::Jsonb
        }
        None => false,
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
    /// `(source column id, bucket)` -> key/value slots of an overflow bucket.
    overflow_slots: HashMap<(ColumnId, usize), (VirtualReadSlotId, VirtualReadSlotId)>,
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

    fn add_array_column(&mut self, meta: &VirtualColumnIdWithMeta) -> VirtualReadSlotId {
        let key = PhysicalSlotKey::ParquetLeaf(meta.parquet_column_id);
        if let Some(slot) = self.slots_by_physical_column.get(&key) {
            return *slot;
        }
        let slot = self.push_slot(VirtualReadSlot {
            offset: meta.meta.offset,
            len: meta.meta.len,
            num_values: meta.meta.num_values,
            // Preserve nullable elements; the scalar physical-type encoding
            // alone cannot describe the complete list's definition levels.
            data_type: meta.data_type.clone(),
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

    fn add_overflow_bucket(
        &mut self,
        source_column_id: ColumnId,
        bucket: usize,
        meta: &VirtualOverflowBucketMeta,
    ) -> (VirtualReadSlotId, VirtualReadSlotId) {
        let overflow_key = (source_column_id, bucket);
        if let Some(slots) = self.overflow_slots.get(&overflow_key) {
            return *slots;
        }
        // Like shared maps, the key/value slots of a bucket are adjacent and
        // decoded as one `Map(String, Variant)` column.
        let key_slot = self.push_slot(VirtualReadSlot {
            offset: meta.key_offset,
            len: meta.key_len,
            num_values: meta.num_values,
            data_type: DataType::String,
        });
        let value_slot = self.push_slot(VirtualReadSlot {
            offset: meta.value_offset,
            len: meta.value_len,
            num_values: meta.num_values,
            data_type: DataType::Variant,
        });
        self.overflow_slots
            .insert(overflow_key, (key_slot, value_slot));
        (key_slot, value_slot)
    }

    fn push_slot(&mut self, slot: VirtualReadSlot) -> VirtualReadSlotId {
        let id = VirtualReadSlotId(self.slots.len() as u32);
        self.slots.push(slot);
        id
    }
}

/// Builds the read plans of one trie node and its subtree.
///
/// Direct paths may store JSON null, `{}` and `[]` in the overflow map, and a
/// node without a leaf may have only overflow values; `overflow` decides how
/// their plans merge the overflow map. Child nodes always read it, because
/// object reconstruction needs every value.
#[allow(clippy::too_many_arguments)]
fn build_plans_for_node(
    node: &VirtualColumnNode,
    source_column_id: ColumnId,
    segments: &[String],
    virtual_meta: &VirtualColumnFileMeta,
    block_meta: &VirtualBlockMeta,
    projected_virtual_schema: Option<&ProjectedVirtualSegmentSchema>,
    slot_builder: &mut VirtualReadSlotBuilder,
    overflow: OverflowRead,
) -> Result<Vec<VirtualFieldReadPlan>> {
    let mut plans = Vec::new();

    let canonical_path = OwnedKeyPaths {
        paths: segments.iter().cloned().map(OwnedKeyPath::Name).collect(),
    }
    .to_canonical_path();
    let mut leaf_plan = None;
    // Only direct paths and leafless nodes have overflow values.
    let mut has_overflow = node.leaf.is_none();
    let mut value_state_mask = None;
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
                if matches!(overflow, OverflowRead::Element) {
                    let slot = slot_builder.add_array_column(meta);
                    leaf_plan = Some(VirtualFieldReadPlan::Direct { slot });
                    has_overflow = false;
                } else {
                    has_overflow = true;
                    value_state_mask = block_value_state_mask(
                        source_column_id,
                        &canonical_path,
                        block_meta,
                        projected_virtual_schema,
                    );
                    let column_meta = direct_virtual_column_meta(
                        source_column_id,
                        &canonical_path,
                        meta,
                        block_meta,
                        projected_virtual_schema,
                    )?;
                    let slot = slot_builder.add_parquet_column(meta, &column_meta);
                    leaf_plan = Some(VirtualFieldReadPlan::Direct { slot });
                }
            }
            VirtualColumnNameIndex::Shared(index) => {
                if let Some((key_slot, value_slot)) = slot_builder.add_shared_columns(
                    &virtual_meta.typed_shared_column_metas,
                    source_column_id,
                    VirtualColumnSharedDataType::Jsonb,
                ) {
                    leaf_plan = Some(VirtualFieldReadPlan::Shared {
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
                    leaf_plan = Some(VirtualFieldReadPlan::Shared {
                        key_slot,
                        value_slot,
                        index: *index,
                    });
                }
            }
        }
    }

    let leaf_plan = if has_overflow {
        with_overflow(
            leaf_plan,
            value_state_mask,
            source_column_id,
            &canonical_path,
            block_meta,
            overflow,
            slot_builder,
        )
    } else {
        leaf_plan
    };
    plans.extend(leaf_plan);

    if let Some(element) = node.element.as_deref() {
        let element_plans = build_plans_for_node(
            element,
            source_column_id,
            &[],
            virtual_meta,
            block_meta,
            projected_virtual_schema,
            slot_builder,
            OverflowRead::Element,
        )?;
        if let Some(element_plan) = coalesce_read_plans(element_plans) {
            let boundary_slot = array_element_boundary(&element_plan, slot_builder)?;
            plans.push(VirtualFieldReadPlan::ArrayGroup {
                boundary_slot,
                element: Box::new(element_plan),
                index: None,
            });
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
        // Empty object keys are valid JSON keys, including in array elements.
        let child_key = segment_name.clone();
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
            if matches!(overflow, OverflowRead::Element) {
                OverflowRead::Element
            } else {
                OverflowRead::Always
            },
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

/// Reuse the smallest leaf already needed by the element plan for boundaries;
/// no dedicated length column or extra sibling leaf needs to be read.
fn array_element_boundary(
    plan: &VirtualFieldReadPlan,
    slots: &VirtualReadSlotBuilder,
) -> Result<VirtualReadSlotId> {
    match plan {
        VirtualFieldReadPlan::Direct { slot } => Ok(*slot),
        VirtualFieldReadPlan::FromParent { parent, .. } => array_element_boundary(parent, slots),
        VirtualFieldReadPlan::Coalesce { plans } => smallest_array_boundary(plans.iter(), slots),
        VirtualFieldReadPlan::Object { entries } => {
            smallest_array_boundary(entries.iter().map(|(_, plan)| plan), slots)
        }
        _ => Err(ErrorCode::Internal("invalid virtual array element plan")),
    }
}

fn smallest_array_boundary<'a>(
    plans: impl Iterator<Item = &'a VirtualFieldReadPlan>,
    slots: &VirtualReadSlotBuilder,
) -> Result<VirtualReadSlotId> {
    let mut smallest = None;
    for plan in plans {
        let candidate = array_element_boundary(plan, slots)?;
        let size = |slot: VirtualReadSlotId| (slots.slots[slot.as_usize()].len, slot);
        if smallest.is_none_or(|slot| size(candidate) < size(slot)) {
            smallest = Some(candidate);
        }
    }
    smallest.ok_or_else(|| ErrorCode::Internal("virtual array group has no element columns"))
}

/// Resolve the first array index against a group, then plan the remaining path
/// within its element space. Array fields are complete typed scalar Array or
/// Jsonb leaves, never recursively expanded by the current writer.
#[allow(clippy::too_many_arguments)]
fn build_array_index_plan(
    root: &VirtualColumnNode,
    source_column_id: ColumnId,
    path: &OwnedKeyPaths,
    virtual_meta: &VirtualColumnFileMeta,
    block_meta: &VirtualBlockMeta,
    schema: Option<&ProjectedVirtualSegmentSchema>,
    slots: &mut VirtualReadSlotBuilder,
) -> Result<Option<VirtualFieldReadPlan>> {
    let mut node = root;
    for (position, key) in path.paths.iter().enumerate() {
        match key {
            OwnedKeyPath::Name(name) => {
                let Some(id) = virtual_meta.string_table.iter().position(|key| key == name) else {
                    return Ok(None);
                };
                let Some(child) = node.children.get(&(id as u32)) else {
                    return Ok(None);
                };
                node = child;
            }
            OwnedKeyPath::Index(index) => {
                let Some(element) = node.element.as_deref() else {
                    return Ok(None);
                };
                let suffix = &path.paths[position + 1..];
                let mut target = element;
                let mut consumed = 0;
                let mut parent = None;
                loop {
                    if let Some(VirtualColumnNameIndex::Column(id)) = target.leaf.as_ref()
                        && let Some(meta) = virtual_meta.column_metas.get(*id as usize)
                        && matches!(meta.data_type.remove_nullable(), DataType::Array(inner)
                            if matches!(inner.remove_nullable(), DataType::Variant | DataType::Array(_)))
                    {
                        parent = Some((target, consumed));
                    }
                    let Some(OwnedKeyPath::Name(name)) = suffix.get(consumed) else {
                        break;
                    };
                    let Some(id) = virtual_meta.string_table.iter().position(|key| key == name)
                    else {
                        break;
                    };
                    let Some(child) = target.children.get(&(id as u32)) else {
                        break;
                    };
                    target = child;
                    consumed += 1;
                }
                let mut plans = Vec::new();
                if consumed == suffix.len() {
                    plans.extend(build_plans_for_node(
                        target,
                        source_column_id,
                        &[],
                        virtual_meta,
                        block_meta,
                        schema,
                        slots,
                        OverflowRead::Element,
                    )?);
                }
                if let Some((parent, depth)) = parent
                    && depth < suffix.len()
                    && let Some(parent) = coalesce_read_plans(build_plans_for_node(
                        parent,
                        source_column_id,
                        &[],
                        virtual_meta,
                        block_meta,
                        schema,
                        slots,
                        OverflowRead::Element,
                    )?)
                {
                    plans.push(VirtualFieldReadPlan::FromParent {
                        parent: Box::new(parent),
                        suffix_path: format_runtime_keypaths(&OwnedKeyPaths {
                            paths: suffix[depth..].to_vec(),
                        }),
                    });
                }
                let Some(plan) = coalesce_read_plans(plans) else {
                    return Ok(Some(VirtualFieldReadPlan::Missing));
                };
                // When the selected element plan already reads a list leaf,
                // use it for boundaries too: items[0].k reads just one leaf.
                let boundary_slot = array_element_boundary(&plan, slots)?;
                return Ok(Some(VirtualFieldReadPlan::ArrayGroup {
                    boundary_slot,
                    element: Box::new(plan),
                    index: Some(*index),
                }));
            }
        }
    }
    Ok(None)
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
