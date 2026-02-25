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
use std::ops::BitOr;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::RecordBatch;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::eval_function;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::metrics_inc_variant_shredding_read_typed_value_hits;
use databend_common_metrics::storage::metrics_inc_variant_shredding_read_typed_value_misses;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_pruner::VirtualColumnReadPlan;
use databend_storages_common_table_meta::meta::Compression;
use jsonb::keypath::OwnedKeyPath as JsonbOwnedKeyPath;
use jsonb::keypath::OwnedKeyPaths as JsonbOwnedKeyPaths;
use parquet::arrow::arrow_reader::RowSelection;

use super::VirtualColumnReader;
use crate::BlockReadResult;
use crate::io::VariantShreddedColumn;
use crate::io::parquet_variant::OwnedKeyPath;
use crate::io::parquet_variant::OwnedKeyPaths;
use crate::io::read::block::parquet::column_chunks_to_record_batch;

pub struct VirtualBlockReadResult {
    pub num_rows: usize,
    pub compression: Compression,
    pub data: BlockReadResult,
    pub schema: TableSchemaRef,
    pub virtual_column_read_plan: BTreeMap<ColumnId, Vec<VirtualColumnReadPlan>>,
    pub is_inline: bool,
    pub shredded_columns: Option<Vec<VariantShreddedColumn>>,
    // Source columns that can be ignored without reading
    pub ignore_column_ids: Option<HashSet<ColumnId>>,
}

impl VirtualBlockReadResult {
    pub fn create(
        num_rows: usize,
        compression: Compression,
        data: BlockReadResult,
        schema: TableSchemaRef,
        virtual_column_read_plan: BTreeMap<ColumnId, Vec<VirtualColumnReadPlan>>,
        is_inline: bool,
        shredded_columns: Option<Vec<VariantShreddedColumn>>,
        ignore_column_ids: Option<HashSet<ColumnId>>,
    ) -> VirtualBlockReadResult {
        VirtualBlockReadResult {
            num_rows,
            compression,
            data,
            schema,
            virtual_column_read_plan,
            is_inline,
            shredded_columns,
            ignore_column_ids,
        }
    }
}

impl VirtualColumnReader {
    pub async fn read_parquet_data_by_merge_io(
        &self,
        read_settings: &ReadSettings,
        virtual_block_meta: &Option<&VirtualBlockMetaIndex>,
        num_rows: usize,
    ) -> Option<VirtualBlockReadResult> {
        let Some(virtual_block_meta) = virtual_block_meta else {
            return None;
        };

        let mut schema = TableSchema::empty();
        let mut ranges = Vec::with_capacity(virtual_block_meta.virtual_column_metas.len());
        let mut shredded_columns = None;

        if virtual_block_meta.is_inline {
            let mut columns =
                Vec::with_capacity(self.virtual_column_info.virtual_column_fields.len());
            for field in &self.virtual_column_info.virtual_column_fields {
                let Some(meta) = virtual_block_meta
                    .virtual_column_metas
                    .get(&field.column_id)
                else {
                    continue;
                };
                let Some(key_paths) = to_parquet_key_paths(&field.key_paths) else {
                    continue;
                };
                let Some(data_type) = virtual_meta_to_arrow(meta.data_type()) else {
                    continue;
                };

                let (offset, len) = meta.offset_length();
                ranges.push((field.column_id, offset..(offset + len)));
                let name = format!("{}", field.column_id);
                schema.add_internal_field(&name, meta.data_type(), field.column_id);

                columns.push(VariantShreddedColumn {
                    source_column_id: field.source_column_id,
                    column_id: field.column_id,
                    key_paths,
                    data_type,
                });
            }
            if !columns.is_empty() {
                shredded_columns = Some(columns);
            }
        } else {
            let mut shared_value_ids = HashSet::new();
            let mut base_id_to_source = HashMap::new();
            for (source_column_id, base_id) in &virtual_block_meta.shared_virtual_column_ids {
                shared_value_ids.insert(*base_id + 1);
                base_id_to_source.insert(*base_id, *source_column_id);
            }
            for (column_id, virtual_column_meta) in &virtual_block_meta.virtual_column_metas {
                let (offset, len) = virtual_column_meta.offset_length();
                ranges.push((*column_id, offset..(offset + len)));
                if shared_value_ids.contains(column_id) {
                    continue;
                }
                if let Some(source_column_id) = base_id_to_source.get(column_id) {
                    let name = format!("{}__shared__", source_column_id);
                    let data_type = TableDataType::Map(Box::new(TableDataType::Tuple {
                        fields_name: vec!["key".to_string(), "value".to_string()],
                        fields_type: vec![
                            TableDataType::Number(NumberDataType::UInt32),
                            TableDataType::Variant,
                        ],
                    }));
                    schema.add_internal_field(&name, data_type, *column_id);
                } else {
                    let name = column_id.to_string();
                    let data_type = virtual_column_meta.data_type();
                    schema.add_internal_field(&name, data_type, *column_id);
                }
            }
        }

        let virtual_loc = &virtual_block_meta.virtual_block_location;
        if ranges.is_empty() {
            return None;
        }
        let merge_io_result =
            MergeIOReader::merge_io_read(read_settings, self.dal.clone(), virtual_loc, &ranges)
                .await
                .ok()?;

        let block_read_res = BlockReadResult::create(merge_io_result, vec![], vec![]);
        let ignore_column_ids = if virtual_block_meta.is_inline {
            None
        } else {
            self.generate_ignore_column_ids(&virtual_block_meta.ignored_source_column_ids)
        };

        Some(VirtualBlockReadResult::create(
            num_rows,
            self.compression.into(),
            block_read_res,
            Arc::new(schema),
            virtual_block_meta.virtual_column_read_plan.clone(),
            virtual_block_meta.is_inline,
            shredded_columns,
            ignore_column_ids,
        ))
    }

    pub fn deserialize_virtual_columns(
        &self,
        mut data_block: DataBlock,
        virtual_data: Option<VirtualBlockReadResult>,
        row_selection: Option<RowSelection>,
    ) -> Result<DataBlock> {
        let orig_schema = virtual_data
            .as_ref()
            .map(|virtual_data| virtual_data.schema.clone())
            .unwrap_or_default();
        let is_inline = virtual_data.as_ref().map(|v| v.is_inline).unwrap_or(false);
        let virtual_column_read_plan = virtual_data
            .as_ref()
            .map(|virtual_data| virtual_data.virtual_column_read_plan.clone())
            .unwrap_or_default();
        let record_batch = virtual_data
            .as_ref()
            .map(|virtual_data| {
                let columns_chunks = virtual_data.data.columns_chunks()?;
                if virtual_data.is_inline {
                    column_chunks_to_record_batch(
                        self.source_schema.as_ref(),
                        virtual_data.num_rows,
                        &columns_chunks,
                        &virtual_data.compression,
                        row_selection,
                        true,
                        virtual_data.shredded_columns.as_deref(),
                    )
                } else {
                    column_chunks_to_record_batch(
                        &virtual_data.schema,
                        virtual_data.num_rows,
                        &columns_chunks,
                        &virtual_data.compression,
                        row_selection,
                        false,
                        None,
                    )
                }
            })
            .transpose()?;

        // If the virtual column has already generated, add it directly,
        // otherwise extract it from the source column
        let func_ctx = self.ctx.get_function_context()?;
        for virtual_column_field in self.virtual_column_info.virtual_column_fields.iter() {
            if !is_inline {
                if let (Some(plans), Some(record_batch)) = (
                    virtual_column_read_plan.get(&virtual_column_field.column_id),
                    record_batch.as_ref(),
                ) {
                    let target_type: DataType = virtual_column_field.data_type.as_ref().into();
                    let cast_func_name = format!(
                        "to_{}",
                        target_type.remove_nullable().to_string().to_lowercase()
                    );
                    let mut args = Vec::new();
                    for plan in plans {
                        let Some((value, data_type)) = eval_read_plan(
                            plan,
                            record_batch,
                            &orig_schema,
                            &func_ctx,
                            data_block.num_rows(),
                        )?
                        else {
                            continue;
                        };
                        let (value, data_type) = if data_type != target_type {
                            eval_function(
                                None,
                                &cast_func_name,
                                [(value, data_type)],
                                &func_ctx,
                                data_block.num_rows(),
                                &BUILTIN_FUNCTIONS,
                            )?
                        } else {
                            (value, data_type)
                        };
                        args.push((value, data_type));
                    }

                    if !args.is_empty() {
                        let (value, data_type) = if args.len() == 1 {
                            args.pop().unwrap()
                        } else {
                            let mut if_args = Vec::with_capacity(args.len() * 2 - 1);
                            let last_index = args.len() - 1;
                            for (idx, (value, data_type)) in args.into_iter().enumerate() {
                                if idx == last_index {
                                    if_args.push((value, data_type));
                                    break;
                                }
                                let (cond, cond_type) = eval_function(
                                    None,
                                    "is_not_null",
                                    [(value.clone(), data_type.clone())],
                                    &func_ctx,
                                    data_block.num_rows(),
                                    &BUILTIN_FUNCTIONS,
                                )?;
                                let (nonnull_value, nonnull_type) = eval_function(
                                    None,
                                    "assume_not_null",
                                    [(value, data_type)],
                                    &func_ctx,
                                    data_block.num_rows(),
                                    &BUILTIN_FUNCTIONS,
                                )?;
                                if_args.push((cond, cond_type));
                                if_args.push((nonnull_value, nonnull_type));
                            }
                            eval_function(
                                None,
                                "if",
                                if_args,
                                &func_ctx,
                                data_block.num_rows(),
                                &BUILTIN_FUNCTIONS,
                            )?
                        };
                        data_block.add_value(value, data_type);
                        continue;
                    }
                }
            }

            let name = format!("{}", virtual_column_field.column_id);
            if let Some(record_batch) = record_batch.as_ref() {
                if is_inline {
                    let mut should_track_hit = false;
                    let inline_column = to_parquet_key_paths(&virtual_column_field.key_paths)
                        .and_then(|key_paths| {
                            should_track_hit = true;
                            extract_typed_value_array(
                                record_batch,
                                &virtual_column_field.source_name,
                                &key_paths,
                            )
                        });
                    if should_track_hit {
                        if inline_column.is_some() {
                            metrics_inc_variant_shredding_read_typed_value_hits(1);
                        } else {
                            metrics_inc_variant_shredding_read_typed_value_misses(1);
                        }
                    }

                    if let Some(arrow_array) = inline_column {
                        // Inline shredding fixes the typed_value schema for the writer.
                        // A block (or a row selection) can therefore produce a typed_value
                        // column that exists in the schema but is entirely NULL for the rows
                        // we are reading. In that case the typed_value column is not
                        // authoritative (the path may be absent or non-scalar in this block),
                        // so we must fall back to get_by_keypath to preserve semantics.
                        // TODO: persist per-block scalar-safe / value-all-null markers so we
                        // can trust typed_value even when it is all NULL.
                        if arrow_array.null_count() != arrow_array.len() {
                            let Ok(orig_field) = orig_schema.field_with_name(&name) else {
                                continue;
                            };
                            let orig_type: DataType = orig_field.data_type().into();
                            let column = Column::from_arrow_rs(arrow_array, &orig_type)?;
                            let data_type: DataType =
                                virtual_column_field.data_type.as_ref().into();
                            if orig_type != data_type {
                                let cast_func_name = format!(
                                    "to_{}",
                                    data_type.remove_nullable().to_string().to_lowercase()
                                );
                                let (cast_value, cast_data_type) = eval_function(
                                    None,
                                    &cast_func_name,
                                    [(Value::Column(column), orig_type)],
                                    &func_ctx,
                                    data_block.num_rows(),
                                    &BUILTIN_FUNCTIONS,
                                )?;
                                data_block.add_value(cast_value, cast_data_type);
                            } else {
                                data_block.add_column(column);
                            };
                            continue;
                        }
                    }
                }
            }

            let src_index = self
                .source_schema
                .index_of(&virtual_column_field.source_name)
                .unwrap();
            let source = data_block.get_by_offset(src_index);
            let src_arg = (source.value(), source.data_type());
            let path_arg = (
                Value::Scalar(Scalar::String(virtual_column_field.key_paths.to_string())),
                DataType::String,
            );

            let (value, data_type) = eval_function(
                None,
                "get_by_keypath",
                [src_arg, path_arg],
                &func_ctx,
                data_block.num_rows(),
                &BUILTIN_FUNCTIONS,
            )?;

            if let Some(cast_func_name) = &virtual_column_field.cast_func_name {
                let (cast_value, cast_data_type) = eval_function(
                    None,
                    cast_func_name,
                    [(value, data_type)],
                    &func_ctx,
                    data_block.num_rows(),
                    &BUILTIN_FUNCTIONS,
                )?;
                data_block.add_value(cast_value, cast_data_type);
            } else {
                data_block.add_value(value, data_type);
            };
        }

        Ok(data_block)
    }
}

fn column_from_record_batch(
    record_batch: &RecordBatch,
    orig_schema: &TableSchema,
    name: &str,
) -> Result<Option<(Column, DataType)>> {
    let Some(arrow_array) = record_batch.column_by_name(name).cloned() else {
        return Ok(None);
    };
    let Ok(orig_field) = orig_schema.field_with_name(name) else {
        return Ok(None);
    };
    let orig_type: DataType = orig_field.data_type().into();
    let column = Column::from_arrow_rs(arrow_array, &orig_type)?;
    Ok(Some((column, orig_type)))
}

fn eval_read_plan(
    plan: &VirtualColumnReadPlan,
    record_batch: &RecordBatch,
    orig_schema: &TableSchema,
    func_ctx: &FunctionContext,
    num_rows: usize,
) -> Result<Option<(Value<AnyType>, DataType)>> {
    match plan {
        VirtualColumnReadPlan::Direct { name } => {
            let Some((column, data_type)) =
                column_from_record_batch(record_batch, orig_schema, name)?
            else {
                return Ok(None);
            };
            Ok(Some((Value::Column(column), data_type)))
        }
        VirtualColumnReadPlan::FromParent {
            parent,
            suffix_path,
        } => {
            let Some((value, data_type)) =
                eval_read_plan(parent, record_batch, orig_schema, func_ctx, num_rows)?
            else {
                return Ok(None);
            };
            if suffix_path.is_empty() {
                return Ok(Some((value, data_type)));
            }
            let (value, value_type) = eval_function(
                None,
                "get_by_keypath",
                [
                    (value, data_type),
                    (
                        Value::Scalar(Scalar::String(suffix_path.clone())),
                        DataType::String,
                    ),
                ],
                func_ctx,
                num_rows,
                &BUILTIN_FUNCTIONS,
            )?;
            Ok(Some((value, value_type)))
        }
        VirtualColumnReadPlan::Shared {
            source_column_id,
            index,
        } => {
            let name = format!("{}__shared__", source_column_id);
            let Some((column, data_type)) =
                column_from_record_batch(record_batch, orig_schema, &name)?
            else {
                return Ok(None);
            };
            let (value, value_type) = eval_function(
                None,
                "get",
                [
                    (Value::Column(column), data_type),
                    (
                        Value::Scalar(Scalar::Number(NumberScalar::UInt32(*index))),
                        DataType::Number(NumberDataType::UInt32),
                    ),
                ],
                func_ctx,
                num_rows,
                &BUILTIN_FUNCTIONS,
            )?;
            Ok(Some((value, value_type)))
        }
        VirtualColumnReadPlan::Object { entries } => {
            if entries.is_empty() {
                return Ok(None);
            }
            let mut args = Vec::with_capacity(entries.len() * 2);
            // Aggregate non-null flags via bitmap OR to check whether all columns are NULL.
            let mut any_not_null_bitmap = MutableBitmap::from_len_zeroed(num_rows);
            for (key, plan) in entries {
                let Some((value, data_type)) =
                    eval_read_plan(plan, record_batch, orig_schema, func_ctx, num_rows)?
                else {
                    return Ok(None);
                };
                let not_null_bitmap = match &value {
                    Value::Column(Column::Nullable(box nullable)) => nullable.validity.clone(),
                    Value::Scalar(Scalar::Null) | Value::Column(Column::Null { .. }) => {
                        Bitmap::new_zeroed(num_rows)
                    }
                    _ => Bitmap::new_trued(num_rows),
                };
                any_not_null_bitmap = any_not_null_bitmap.bitor(&not_null_bitmap);
                args.push((Value::Scalar(Scalar::String(key.clone())), DataType::String));
                args.push((value, data_type));
            }
            let (value, value_type) = eval_function(
                None,
                "object_construct",
                args,
                func_ctx,
                num_rows,
                &BUILTIN_FUNCTIONS,
            )?;
            // If all columns in Object are NULL, return NULL instead of empty Object.
            let has_any = Value::Column(Column::Boolean(any_not_null_bitmap.into()));
            let has_any_type = DataType::Boolean;
            let null_type = value_type.wrap_nullable();
            let (value, value_type) = eval_function(
                None,
                "if",
                [
                    (has_any, has_any_type),
                    (value, value_type),
                    (Value::Scalar(Scalar::Null), null_type),
                ],
                func_ctx,
                num_rows,
                &BUILTIN_FUNCTIONS,
            )?;
            Ok(Some((value, value_type)))
        }
    }
}

fn virtual_meta_to_arrow(data_type: TableDataType) -> Option<arrow_schema::DataType> {
    match data_type.remove_nullable() {
        TableDataType::Boolean => Some(arrow_schema::DataType::Boolean),
        TableDataType::Number(NumberDataType::UInt64) => Some(arrow_schema::DataType::UInt64),
        TableDataType::Number(NumberDataType::Int64) => Some(arrow_schema::DataType::Int64),
        TableDataType::Number(NumberDataType::Float64) => Some(arrow_schema::DataType::Float64),
        TableDataType::String => Some(arrow_schema::DataType::Utf8View),
        _ => None,
    }
}

fn extract_typed_value_array(
    record_batch: &arrow_array::RecordBatch,
    source_name: &str,
    key_paths: &OwnedKeyPaths,
) -> Option<arrow_array::ArrayRef> {
    let variant_array = record_batch.column_by_name(source_name)?;
    let struct_array = variant_array
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()?;
    let mut typed_value = struct_array.column_by_name("typed_value")?.clone();

    for path in &key_paths.paths {
        let name_owned;
        let name = match path {
            OwnedKeyPath::Name(name) => name.as_str(),
            OwnedKeyPath::Index(idx) => {
                name_owned = idx.to_string();
                name_owned.as_str()
            }
        };
        let typed_struct = typed_value
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()?;
        let field_array = typed_struct.column_by_name(name)?;
        let shredded_struct = field_array
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()?;
        typed_value = shredded_struct.column_by_name("typed_value")?.clone();
    }

    Some(typed_value)
}

fn to_parquet_key_paths(key_paths: &JsonbOwnedKeyPaths) -> Option<OwnedKeyPaths> {
    if key_paths.paths.is_empty() {
        return None;
    }
    let mut paths = Vec::with_capacity(key_paths.paths.len());
    for path in &key_paths.paths {
        match path {
            JsonbOwnedKeyPath::Index(idx) => paths.push(OwnedKeyPath::Index(*idx)),
            JsonbOwnedKeyPath::Name(name) => paths.push(OwnedKeyPath::Name(name.clone())),
            JsonbOwnedKeyPath::QuotedName(name) => paths.push(OwnedKeyPath::Name(name.clone())),
        }
    }
    Some(OwnedKeyPaths { paths })
}
