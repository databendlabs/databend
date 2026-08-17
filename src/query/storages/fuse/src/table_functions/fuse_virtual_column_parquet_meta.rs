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

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::VirtualColumnIdWithMeta;
use databend_storages_common_index::VirtualColumnNameIndex;
use databend_storages_common_index::VirtualColumnNode;
use databend_storages_common_index::VirtualColumnSharedDataType;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractBlockMeta;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use jsonb::keypath::OwnedKeyPath;
use jsonb::keypath::OwnedKeyPaths;
use log::warn;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::io::read::ColumnOrientedSegmentReader;
use crate::io::read::RowOrientedSegmentReader;
use crate::io::read::SegmentReader;
use crate::io::read::load_virtual_column_file_meta;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFuncTemplate;
use crate::table_functions::function_template::TableMetaFunc;
use crate::table_functions::fuse_block_statistics::build_variant;

pub struct FuseVirtualColumnParquetMeta;
pub type FuseVirtualColumnParquetMetaFunc = TableMetaFuncTemplate<FuseVirtualColumnParquetMeta>;

#[async_trait::async_trait]
impl TableMetaFunc for FuseVirtualColumnParquetMeta {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("virtual_location", TableDataType::String),
            TableField::new(
                "virtual_column_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("direct_columns", TableDataType::Variant),
            TableField::new("shared_columns", TableDataType::Variant),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        match tbl.is_column_oriented() {
            true => {
                Self::apply_generic::<ColumnOrientedSegmentReader>(ctx, tbl, snapshot, limit).await
            }
            false => {
                Self::apply_generic::<RowOrientedSegmentReader>(ctx, tbl, snapshot, limit).await
            }
        }
    }
}

impl FuseVirtualColumnParquetMeta {
    async fn apply_generic<R: SegmentReader>(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let limit = limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(snapshot.summary.block_count as usize, limit);

        let snapshot_id = snapshot.snapshot_id.simple().to_string();
        let timestamp = snapshot.timestamp.unwrap_or_default().timestamp_micros();
        let mut virtual_locations = Vec::with_capacity(len);
        let mut virtual_column_sizes = Vec::with_capacity(len);
        let mut row_counts = Vec::with_capacity(len);
        let mut direct_columns = Vec::with_capacity(len);
        let mut shared_columns = Vec::with_capacity(len);

        let schema = tbl.schema();
        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), schema.clone());
        let source_column_names = build_source_column_name_map(schema.as_ref());
        let func_ctx = ctx.get_function_context()?;
        let mut num_rows = 0;
        let chunk_size =
            std::cmp::min(ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);
        let projection = HashSet::new();
        'outer: for chunk in snapshot.segments.chunks(chunk_size) {
            let chunk_refs = chunk.iter().collect::<Vec<_>>();
            let segments = segments_io
                .generic_read_compact_segments::<R>(&chunk_refs, true, &projection)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block in segment.block_metas()? {
                    let Some(block_meta) = block.virtual_block_meta() else {
                        continue;
                    };
                    let location = block_meta.virtual_location.0;
                    let virtual_meta = match load_virtual_column_file_meta(
                        tbl.operator.clone(),
                        &location,
                    )
                    .await
                    {
                        Ok(meta) => meta,
                        Err(error) => {
                            warn!(
                                "Failed to load virtual column metadata from {}: {}",
                                location, error
                            );
                            continue;
                        }
                    };
                    let (direct, shared) = virtual_column_file_variants(
                        &virtual_meta,
                        &source_column_names,
                        &func_ctx,
                    );
                    virtual_locations.push(location);
                    virtual_column_sizes.push(block_meta.virtual_column_size);
                    row_counts.push(block.row_count());
                    direct_columns.push(direct);
                    shared_columns.push(shared);
                    num_rows += 1;
                    if num_rows >= limit {
                        break 'outer;
                    }
                }
            }
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry::new_const_column_arg::<StringType>(snapshot_id, num_rows),
                BlockEntry::new_const_column_arg::<TimestampType>(timestamp, num_rows),
                StringType::from_data(virtual_locations).into(),
                UInt64Type::from_data(virtual_column_sizes).into(),
                UInt64Type::from_data(row_counts).into(),
                VariantType::from_data(direct_columns).into(),
                VariantType::from_data(shared_columns).into(),
            ],
            num_rows,
        ))
    }
}

pub(crate) fn virtual_column_file_variants(
    virtual_meta: &VirtualColumnFileMeta,
    source_column_names: &HashMap<u32, String>,
    func_ctx: &FunctionContext,
) -> (Vec<u8>, Vec<u8>) {
    let entries = collect_virtual_column_entries(virtual_meta, source_column_names);
    let mut direct = BTreeMap::new();
    let mut shared = BTreeMap::new();
    for entry in entries {
        let value = Scalar::Tuple(vec![
            Scalar::Number(NumberScalar::UInt32(entry.source_column_id)),
            Scalar::String(entry.source_column_name),
            entry
                .column_id
                .map(|value| Scalar::Number(NumberScalar::UInt32(value)))
                .unwrap_or(Scalar::Null),
            Scalar::String(entry.column_type),
            entry
                .offset
                .map(|value| Scalar::Number(NumberScalar::UInt64(value)))
                .unwrap_or(Scalar::Null),
            entry
                .len
                .map(|value| Scalar::Number(NumberScalar::UInt64(value)))
                .unwrap_or(Scalar::Null),
            Scalar::Number(NumberScalar::UInt64(entry.num_values)),
            entry
                .shared_paths
                .map(Scalar::String)
                .unwrap_or(Scalar::Null),
        ]);
        let data_type = TableDataType::Tuple {
            fields_name: vec![
                "source_column_id".to_string(),
                "source_column_name".to_string(),
                "parquet_column_id".to_string(),
                "type".to_string(),
                "offset".to_string(),
                "length".to_string(),
                "num_values".to_string(),
                "paths".to_string(),
            ],
            fields_type: vec![
                TableDataType::Number(NumberDataType::UInt32),
                TableDataType::String,
                TableDataType::Number(NumberDataType::UInt32).wrap_nullable(),
                TableDataType::String,
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
                TableDataType::Number(NumberDataType::UInt64),
                TableDataType::String.wrap_nullable(),
            ],
        };
        if entry.storage_kind == "direct" {
            direct.insert(entry.column_name, (value, data_type));
        } else {
            shared.insert(entry.column_name, (value, data_type));
        }
    }
    (
        build_named_tuple_variant(direct.into_iter().collect(), func_ctx),
        build_named_tuple_variant(shared.into_iter().collect(), func_ctx),
    )
}

pub(crate) struct VirtualColumnEntry {
    pub(crate) source_column_id: u32,
    pub(crate) source_column_name: String,
    pub(crate) storage_kind: &'static str,
    pub(crate) shared_paths: Option<String>,
    pub(crate) column_name: String,
    pub(crate) column_type: String,
    pub(crate) column_id: Option<u32>,
    pub(crate) offset: Option<u64>,
    pub(crate) len: Option<u64>,
    pub(crate) num_values: u64,
}

struct SharedPathBucket {
    source_column_name: String,
    paths: Vec<String>,
}

pub(crate) fn collect_virtual_column_entries(
    virtual_meta: &VirtualColumnFileMeta,
    source_column_names: &HashMap<u32, String>,
) -> Vec<VirtualColumnEntry> {
    let mut entries = Vec::new();
    let mut shared_buckets = HashMap::new();

    for (source_column_id, node) in &virtual_meta.virtual_column_nodes {
        let source_column_name = source_column_names
            .get(source_column_id)
            .cloned()
            .unwrap_or_else(|| source_column_id.to_string());
        let mut key_paths = OwnedKeyPaths { paths: Vec::new() };
        collect_virtual_column_leaves(
            virtual_meta,
            *source_column_id,
            &source_column_name,
            node,
            &mut key_paths,
            &mut entries,
            &mut shared_buckets,
        );
    }

    for ((source_column_id, data_type), mut bucket) in shared_buckets {
        bucket.paths.sort();
        let metas = virtual_meta
            .typed_shared_column_metas
            .get(&source_column_id)
            .and_then(|typed_metas| typed_metas.get(&data_type));
        if let Some((key_meta, value_meta)) = metas {
            push_shared_entries(
                source_column_id,
                &bucket.source_column_name,
                data_type,
                &bucket.paths,
                key_meta,
                value_meta,
                &mut entries,
            );
        }
    }

    entries.sort_by(|a, b| {
        a.column_id
            .unwrap_or(u32::MAX)
            .cmp(&b.column_id.unwrap_or(u32::MAX))
            .then_with(|| a.column_name.cmp(&b.column_name))
    });
    entries
}

fn shared_column_name(source_name: &str, data_type: VirtualColumnSharedDataType) -> String {
    let suffix = match data_type {
        VirtualColumnSharedDataType::Boolean => "__shared_bool_virtual_column_data__",
        VirtualColumnSharedDataType::UInt64 => "__shared_uint64_virtual_column_data__",
        VirtualColumnSharedDataType::Int64 => "__shared_int64_virtual_column_data__",
        VirtualColumnSharedDataType::Float64 => "__shared_float64_virtual_column_data__",
        VirtualColumnSharedDataType::String => "__shared_string_virtual_column_data__",
        VirtualColumnSharedDataType::Jsonb => "__shared_virtual_column_data__",
    };
    format!("{source_name}.{suffix}")
}

fn push_shared_entries(
    source_column_id: u32,
    source_column_name: &str,
    data_type: VirtualColumnSharedDataType,
    paths: &[String],
    key_meta: &VirtualColumnIdWithMeta,
    value_meta: &VirtualColumnIdWithMeta,
    entries: &mut Vec<VirtualColumnEntry>,
) {
    let column_name = shared_column_name(source_column_name, data_type);
    let shared_paths = paths.join(", ");
    entries.push(VirtualColumnEntry {
        source_column_id,
        source_column_name: source_column_name.to_string(),
        storage_kind: "shared_key",
        shared_paths: Some(shared_paths),
        column_name: format!("{column_name}.entries.key"),
        column_type: key_meta.data_type.to_string(),
        column_id: Some(key_meta.parquet_column_id),
        offset: Some(key_meta.meta.offset),
        len: Some(key_meta.meta.len),
        num_values: key_meta.meta.num_values,
    });
    entries.push(VirtualColumnEntry {
        source_column_id,
        source_column_name: source_column_name.to_string(),
        storage_kind: "shared_value",
        shared_paths: None,
        column_name: format!("{column_name}.entries.value"),
        column_type: value_meta.data_type.to_string(),
        column_id: Some(value_meta.parquet_column_id),
        offset: Some(value_meta.meta.offset),
        len: Some(value_meta.meta.len),
        num_values: value_meta.meta.num_values,
    });
}

fn push_shared_path(
    shared_buckets: &mut HashMap<(u32, VirtualColumnSharedDataType), SharedPathBucket>,
    source_column_id: u32,
    source_column_name: &str,
    data_type: VirtualColumnSharedDataType,
    virtual_path: String,
) {
    shared_buckets
        .entry((source_column_id, data_type))
        .or_insert_with(|| SharedPathBucket {
            source_column_name: source_column_name.to_string(),
            paths: Vec::new(),
        })
        .paths
        .push(virtual_path);
}

fn collect_virtual_column_leaves(
    virtual_meta: &VirtualColumnFileMeta,
    source_column_id: u32,
    source_column_name: &str,
    node: &VirtualColumnNode,
    key_paths: &mut OwnedKeyPaths,
    entries: &mut Vec<VirtualColumnEntry>,
    shared_buckets: &mut HashMap<(u32, VirtualColumnSharedDataType), SharedPathBucket>,
) {
    if let Some(leaf) = node.leaf.as_ref() {
        let canonical_path = key_paths.to_canonical_path();
        let column_name = format!("{}.{}", source_column_name, &canonical_path);
        match leaf {
            VirtualColumnNameIndex::Column(leaf_index) => {
                if let Some(meta) = virtual_meta.column_metas.get(*leaf_index as usize) {
                    entries.push(VirtualColumnEntry {
                        source_column_id,
                        source_column_name: source_column_name.to_string(),
                        storage_kind: "direct",
                        shared_paths: None,
                        column_name,
                        column_type: meta.data_type.to_string(),
                        column_id: Some(meta.parquet_column_id),
                        offset: Some(meta.meta.offset),
                        len: Some(meta.meta.len),
                        num_values: meta.meta.num_values,
                    });
                }
            }
            VirtualColumnNameIndex::Shared(_) => {
                push_shared_path(
                    shared_buckets,
                    source_column_id,
                    source_column_name,
                    VirtualColumnSharedDataType::Jsonb,
                    canonical_path,
                );
            }
            VirtualColumnNameIndex::TypedShared { data_type, .. } => {
                push_shared_path(
                    shared_buckets,
                    source_column_id,
                    source_column_name,
                    *data_type,
                    canonical_path,
                );
            }
        }
    }

    let mut children: Vec<(u32, &VirtualColumnNode)> = node
        .children
        .iter()
        .map(|(id, child)| (*id, child))
        .collect();
    children.sort_by_key(|(id, _)| *id);
    for (child_id, child_node) in children {
        let Some(segment) = virtual_meta.string_table.get(child_id as usize) else {
            continue;
        };
        key_paths.paths.push(OwnedKeyPath::Name(segment.clone()));
        collect_virtual_column_leaves(
            virtual_meta,
            source_column_id,
            source_column_name,
            child_node,
            key_paths,
            entries,
            shared_buckets,
        );
        key_paths.paths.pop();
    }
}

pub(crate) fn build_source_column_name_map(schema: &TableSchema) -> HashMap<u32, String> {
    schema
        .fields()
        .iter()
        .filter(|field| field.data_type().remove_nullable() == TableDataType::Variant)
        .map(|field| (field.column_id, field.name.clone()))
        .collect()
}

fn build_named_tuple_variant(
    fields: Vec<(String, (Scalar, TableDataType))>,
    func_ctx: &FunctionContext,
) -> Vec<u8> {
    let (names, values): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
    let (scalars, types): (Vec<_>, Vec<_>) = values.into_iter().unzip();

    let scalar = Scalar::Tuple(scalars);
    let data_type = TableDataType::Tuple {
        fields_name: names,
        fields_type: types,
    };
    build_variant(scalar, &data_type, func_ctx)
}
