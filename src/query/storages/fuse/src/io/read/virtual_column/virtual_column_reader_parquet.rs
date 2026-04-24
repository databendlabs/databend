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

use arrow_array::RecordBatch;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::eval_function;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::VirtualColumnSharedDataType;
use databend_storages_common_io::MergeIOReadResult;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::OwnerMemory;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_pruner::VirtualColumnReadPlan;
use databend_storages_common_table_meta::meta::Compression;
use jsonb::OwnedJsonb;
use jsonb::RawJsonb;
use parquet::arrow::arrow_reader::RowSelection;

use super::VirtualColumnReader;
use crate::BlockReadResult;
use crate::io::read::block::parquet::column_chunks_to_record_batch;

pub struct VirtualBlockReadResult {
    pub num_rows: usize,
    pub compression: Compression,
    pub data: BlockReadResult,
    pub schema: TableSchemaRef,
    pub virtual_column_read_plan: BTreeMap<ColumnId, Vec<VirtualColumnReadPlan>>,
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
        ignore_column_ids: Option<HashSet<ColumnId>>,
    ) -> VirtualBlockReadResult {
        VirtualBlockReadResult {
            num_rows,
            compression,
            data,
            schema,
            virtual_column_read_plan,
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
        let mut shared_value_ids = HashSet::new();
        let mut base_id_to_shared = HashMap::new();
        for ((source_column_id, data_type), base_id) in
            &virtual_block_meta.shared_virtual_column_ids
        {
            shared_value_ids.insert(*base_id + 1);
            base_id_to_shared.insert(*base_id, (*source_column_id, *data_type));
        }
        for (column_id, virtual_column_meta) in &virtual_block_meta.virtual_column_metas {
            let (offset, len) = virtual_column_meta.offset_length();
            ranges.push((*column_id, offset..(offset + len)));
            if shared_value_ids.contains(column_id) {
                continue;
            }
            if let Some((source_column_id, shared_data_type)) = base_id_to_shared.get(column_id) {
                let name = shared_internal_name(*source_column_id, *shared_data_type);
                let data_type = TableDataType::Map(Box::new(TableDataType::Tuple {
                    fields_name: vec!["key".to_string(), "value".to_string()],
                    fields_type: vec![
                        TableDataType::Number(NumberDataType::UInt32),
                        infer_schema_type(&shared_value_data_type(*shared_data_type)).unwrap(),
                    ],
                }));
                schema.add_internal_field(&name, data_type, *column_id);
            } else {
                let name = column_id.to_string();
                let data_type = virtual_column_meta.data_type();
                schema.add_internal_field(&name, data_type, *column_id);
            }
        }

        let virtual_loc = &virtual_block_meta.virtual_block_location;
        let merge_io_result = if ranges.is_empty() {
            MergeIOReadResult::create(
                OwnerMemory::create(vec![]),
                HashMap::new(),
                virtual_loc.clone(),
            )
        } else {
            MergeIOReader::merge_io_read(read_settings, self.dal.clone(), virtual_loc, &ranges)
                .await
                .ok()?
        };

        let block_read_res = BlockReadResult::create(merge_io_result, vec![], vec![]);
        let ignore_column_ids =
            self.generate_ignore_column_ids(&virtual_block_meta.ignored_source_column_ids);

        Some(VirtualBlockReadResult::create(
            num_rows,
            self.compression.into(),
            block_read_res,
            Arc::new(schema),
            virtual_block_meta.virtual_column_read_plan.clone(),
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
        let virtual_column_read_plan = virtual_data
            .as_ref()
            .map(|virtual_data| virtual_data.virtual_column_read_plan.clone())
            .unwrap_or_default();
        let record_batch = virtual_data
            .filter(|virtual_data| !virtual_data.schema.fields().is_empty())
            .map(|virtual_data| {
                let columns_chunks = virtual_data.data.columns_chunks()?;
                column_chunks_to_record_batch(
                    &virtual_data.schema,
                    virtual_data.num_rows,
                    &columns_chunks,
                    &virtual_data.compression,
                    row_selection,
                )
            })
            .transpose()?;

        // If the virtual column has already generated, add it directly,
        // otherwise extract it from the source column
        let func_ctx = self.ctx.get_function_context()?;
        for virtual_column_field in self.virtual_column_info.virtual_column_fields.iter() {
            if let Some(plans) = virtual_column_read_plan.get(&virtual_column_field.column_id) {
                let target_type: DataType = virtual_column_field.data_type.as_ref().into();
                let cast_func_name = format!(
                    "to_{}",
                    target_type.remove_nullable().to_string().to_lowercase()
                );
                let mut args = Vec::new();
                for plan in plans {
                    let (value, data_type) = match plan {
                        VirtualColumnReadPlan::Missing => {
                            (Value::Scalar(Scalar::Null), target_type.wrap_nullable())
                        }
                        _ => {
                            let Some(record_batch) = record_batch.as_ref() else {
                                continue;
                            };
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
                            (value, data_type)
                        }
                    };
                    let (value, data_type) =
                        if data_type.remove_nullable() != target_type.remove_nullable() {
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

            let name = format!("{}", virtual_column_field.column_id);
            if let Some(arrow_array) = record_batch
                .as_ref()
                .and_then(|r| r.column_by_name(&name).cloned())
            {
                let orig_field = orig_schema.field_with_name(&name).unwrap();
                let orig_type: DataType = orig_field.data_type().into();
                let column = Column::from_arrow_rs(arrow_array, &orig_type)?;
                let data_type: DataType = virtual_column_field.data_type.as_ref().into();
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
        VirtualColumnReadPlan::Missing => Ok(Some((Value::Scalar(Scalar::Null), DataType::Null))),
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
            data_type: shared_data_type,
            index,
        } => {
            let name = shared_internal_name(*source_column_id, *shared_data_type);
            let Some((column, data_type)) =
                column_from_record_batch(record_batch, orig_schema, &name)?
            else {
                return Ok(None);
            };
            Ok(Some(materialize_shared_map_value(
                column,
                data_type,
                *shared_data_type,
                *index,
                num_rows,
            )?))
        }
        VirtualColumnReadPlan::Coalesce { plans } => {
            materialize_coalesce_read_plan(plans, record_batch, orig_schema, func_ctx, num_rows)
        }
        VirtualColumnReadPlan::Object { entries } => {
            if entries.is_empty() {
                return Ok(None);
            }
            materialize_object_read_plan(entries, record_batch, orig_schema, func_ctx, num_rows)
        }
    }
}

fn materialize_coalesce_read_plan(
    plans: &[VirtualColumnReadPlan],
    record_batch: &RecordBatch,
    orig_schema: &TableSchema,
    func_ctx: &FunctionContext,
    num_rows: usize,
) -> Result<Option<(Value<AnyType>, DataType)>> {
    let mut args = Vec::with_capacity(plans.len());
    for plan in plans {
        let Some((value, data_type)) =
            eval_read_plan(plan, record_batch, orig_schema, func_ctx, num_rows)?
        else {
            continue;
        };
        args.push((value, data_type));
    }

    if args.is_empty() {
        return Ok(None);
    }
    if args.len() == 1 {
        return Ok(Some(args.pop().unwrap()));
    }

    Ok(Some(materialize_coalesce_values(
        &args, func_ctx, num_rows,
    )?))
}

fn materialize_coalesce_values(
    args: &[(Value<AnyType>, DataType)],
    func_ctx: &FunctionContext,
    num_rows: usize,
) -> Result<(Value<AnyType>, DataType)> {
    let output_type = DataType::Nullable(Box::new(DataType::Variant));
    let mut builder = ColumnBuilder::with_capacity(&output_type, num_rows);
    for row in 0..num_rows {
        let mut pushed = false;
        for (value, _) in args {
            let scalar = value.index(row).unwrap_or(ScalarRef::Null);
            if scalar == ScalarRef::Null {
                continue;
            }

            let mut value_buf = Vec::new();
            cast_scalar_to_variant(scalar, &func_ctx.tz, &mut value_buf, None);
            builder.push(ScalarRef::Variant(value_buf.as_slice()));
            pushed = true;
            break;
        }
        if !pushed {
            builder.push(ScalarRef::Null);
        }
    }

    Ok((Value::Column(builder.build()), output_type))
}

fn materialize_object_read_plan(
    entries: &[(String, VirtualColumnReadPlan)],
    record_batch: &RecordBatch,
    orig_schema: &TableSchema,
    func_ctx: &FunctionContext,
    num_rows: usize,
) -> Result<Option<(Value<AnyType>, DataType)>> {
    let mut child_values = Vec::with_capacity(entries.len());
    let mut shared_groups: BTreeMap<(ColumnId, VirtualColumnSharedDataType), Vec<(usize, u32)>> =
        BTreeMap::new();

    for (entry_idx, (_key, plan)) in entries.iter().enumerate() {
        if let VirtualColumnReadPlan::Shared {
            source_column_id,
            data_type,
            index,
        } = plan
        {
            shared_groups
                .entry((*source_column_id, *data_type))
                .or_default()
                .push((entry_idx, *index));
            continue;
        }

        let Some((value, _data_type)) =
            eval_read_plan(plan, record_batch, orig_schema, func_ctx, num_rows)?
        else {
            return Ok(None);
        };
        child_values.push((entries[entry_idx].0.as_str(), value));
    }

    let mut shared_object_groups = Vec::with_capacity(shared_groups.len());
    for ((source_column_id, shared_data_type), requests) in shared_groups {
        let name = shared_internal_name(source_column_id, shared_data_type);
        let Some((column, data_type)) = column_from_record_batch(record_batch, orig_schema, &name)?
        else {
            return Ok(None);
        };
        validate_shared_map_column(&column, &data_type, num_rows)?;

        let mut keys_by_index: HashMap<u32, Vec<&str>> = HashMap::with_capacity(requests.len());
        for (entry_idx, index) in requests {
            keys_by_index
                .entry(index)
                .or_default()
                .push(entries[entry_idx].0.as_str());
        }
        shared_object_groups.push(SharedObjectGroup {
            column,
            keys_by_index,
        });
    }

    Ok(Some(build_object_column(
        &child_values,
        &shared_object_groups,
        func_ctx,
        num_rows,
    )?))
}

struct SharedObjectGroup<'a> {
    column: Column,
    keys_by_index: HashMap<u32, Vec<&'a str>>,
}

fn build_object_column(
    child_values: &[(&str, Value<AnyType>)],
    shared_object_groups: &[SharedObjectGroup],
    func_ctx: &FunctionContext,
    num_rows: usize,
) -> Result<(Value<AnyType>, DataType)> {
    let output_type = DataType::Nullable(Box::new(DataType::Variant));
    let mut builder = ColumnBuilder::with_capacity(&output_type, num_rows);
    let shared_entry_count = shared_object_groups
        .iter()
        .map(|group| group.keys_by_index.values().map(Vec::len).sum::<usize>())
        .sum::<usize>();
    let mut kvs: Vec<(&str, Vec<u8>)> = Vec::with_capacity(child_values.len() + shared_entry_count);
    for row in 0..num_rows {
        kvs.clear();
        for (key, value) in child_values.iter() {
            let scalar = value.index(row).unwrap_or(ScalarRef::Null);
            if scalar == ScalarRef::Null {
                continue;
            }

            let mut value_buf = Vec::new();
            cast_scalar_to_variant(scalar, &func_ctx.tz, &mut value_buf, None);
            kvs.push((*key, value_buf));
        }
        for group in shared_object_groups {
            append_shared_object_group_values(group, row, &mut kvs, func_ctx)?;
        }

        if kvs.is_empty() {
            builder.push(ScalarRef::Null);
            continue;
        }

        let object = OwnedJsonb::build_object(
            kvs.iter()
                .map(|(key, value)| (*key, RawJsonb::new(value.as_slice()))),
        )
        .map_err(|err| {
            ErrorCode::Internal(format!("failed to build virtual column object: {err}"))
        })?;
        builder.push(ScalarRef::Variant(object.as_ref()));
    }

    Ok((Value::Column(builder.build()), output_type))
}

fn append_shared_object_group_values<'a>(
    group: &'a SharedObjectGroup<'a>,
    row: usize,
    kvs: &mut Vec<(&'a str, Vec<u8>)>,
    func_ctx: &FunctionContext,
) -> Result<()> {
    let Column::Map(map) = &group.column else {
        return Err(ErrorCode::Internal(
            "Virtual column shared data should be Map",
        ));
    };
    let Column::Tuple(fields) = map.values() else {
        return Err(ErrorCode::Internal(
            "Virtual column shared map values should be Tuple(key, value)",
        ));
    };
    let Column::Number(NumberColumn::UInt32(keys)) = &fields[0] else {
        return Err(ErrorCode::Internal(
            "Virtual column shared map key should be UInt32",
        ));
    };
    let values = &fields[1];
    let offsets = map.offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;

    for pos in start..end {
        let Some(object_keys) = group.keys_by_index.get(&keys[pos]) else {
            continue;
        };
        let value = values.index(pos).unwrap_or(ScalarRef::Null);
        if value == ScalarRef::Null {
            continue;
        }

        let mut value_buf = Vec::new();
        cast_scalar_to_variant(value, &func_ctx.tz, &mut value_buf, None);
        for object_key in object_keys {
            kvs.push((*object_key, value_buf.clone()));
        }
    }

    Ok(())
}

fn materialize_shared_map_value(
    column: Column,
    data_type: DataType,
    shared_data_type: VirtualColumnSharedDataType,
    index: u32,
    num_rows: usize,
) -> Result<(Value<AnyType>, DataType)> {
    let mut values = materialize_shared_map_values(
        column,
        data_type,
        shared_data_type,
        &[(0, index)],
        num_rows,
    )?;
    let Some((_entry_idx, value, data_type)) = values.pop() else {
        return Err(ErrorCode::Internal(
            "Virtual column shared value is missing",
        ));
    };
    Ok((value, data_type))
}

fn materialize_shared_map_values(
    column: Column,
    data_type: DataType,
    shared_data_type: VirtualColumnSharedDataType,
    requests: &[(usize, u32)],
    num_rows: usize,
) -> Result<Vec<(usize, Value<AnyType>, DataType)>> {
    let Column::Map(map) = column else {
        return Err(ErrorCode::Internal(format!(
            "Virtual column shared data should be Map, but got {:?}",
            data_type
        )));
    };
    if map.len() != num_rows {
        return Err(ErrorCode::Internal(format!(
            "Virtual column shared map row count mismatch, expected {}, got {}",
            num_rows,
            map.len()
        )));
    }

    let Column::Tuple(fields) = map.values() else {
        return Err(ErrorCode::Internal(
            "Virtual column shared map values should be Tuple(key, value)",
        ));
    };
    if fields.len() != 2 {
        return Err(ErrorCode::Internal(format!(
            "Virtual column shared map tuple should have 2 fields, but got {}",
            fields.len()
        )));
    }
    let Column::Number(NumberColumn::UInt32(keys)) = &fields[0] else {
        return Err(ErrorCode::Internal(
            "Virtual column shared map key should be UInt32",
        ));
    };
    let values = &fields[1];
    let offsets = map.offsets();
    let value_type = shared_value_data_type(shared_data_type);
    let output_type = value_type.wrap_nullable();
    let mut request_positions: HashMap<u32, Vec<usize>> = HashMap::with_capacity(requests.len());
    for (request_pos, (_entry_idx, index)) in requests.iter().enumerate() {
        request_positions
            .entry(*index)
            .or_default()
            .push(request_pos);
    }

    let mut builders = (0..requests.len())
        .map(|_| ColumnBuilder::with_capacity(&output_type, num_rows))
        .collect::<Vec<_>>();
    let mut row_values = vec![ScalarRef::Null; requests.len()];

    for row in 0..num_rows {
        row_values.fill(ScalarRef::Null);
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        for pos in start..end {
            if let Some(positions) = request_positions.get(&keys[pos]) {
                let value = values.index(pos).unwrap_or(ScalarRef::Null);
                for request_pos in positions {
                    row_values[*request_pos] = value.clone();
                }
            }
        }
        for (builder, value) in builders.iter_mut().zip(row_values.iter()) {
            builder.push(value.clone());
        }
    }

    Ok(requests
        .iter()
        .zip(builders)
        .map(|((entry_idx, _index), builder)| {
            (
                *entry_idx,
                Value::Column(builder.build()),
                output_type.clone(),
            )
        })
        .collect())
}

fn validate_shared_map_column(
    column: &Column,
    data_type: &DataType,
    num_rows: usize,
) -> Result<()> {
    let Column::Map(map) = column else {
        return Err(ErrorCode::Internal(format!(
            "Virtual column shared data should be Map, but got {:?}",
            data_type
        )));
    };
    if map.len() != num_rows {
        return Err(ErrorCode::Internal(format!(
            "Virtual column shared map row count mismatch, expected {}, got {}",
            num_rows,
            map.len()
        )));
    }

    let Column::Tuple(fields) = map.values() else {
        return Err(ErrorCode::Internal(
            "Virtual column shared map values should be Tuple(key, value)",
        ));
    };
    if fields.len() != 2 {
        return Err(ErrorCode::Internal(format!(
            "Virtual column shared map tuple should have 2 fields, but got {}",
            fields.len()
        )));
    }
    let Column::Number(NumberColumn::UInt32(_keys)) = &fields[0] else {
        return Err(ErrorCode::Internal(
            "Virtual column shared map key should be UInt32",
        ));
    };

    Ok(())
}

fn shared_internal_name(
    source_column_id: ColumnId,
    data_type: VirtualColumnSharedDataType,
) -> String {
    match data_type {
        VirtualColumnSharedDataType::Jsonb => format!("{source_column_id}__shared__"),
        VirtualColumnSharedDataType::Boolean => format!("{source_column_id}__shared_bool__"),
        VirtualColumnSharedDataType::UInt64 => format!("{source_column_id}__shared_uint64__"),
        VirtualColumnSharedDataType::Int64 => format!("{source_column_id}__shared_int64__"),
        VirtualColumnSharedDataType::Float64 => format!("{source_column_id}__shared_float64__"),
        VirtualColumnSharedDataType::String => format!("{source_column_id}__shared_string__"),
    }
}

fn shared_value_data_type(data_type: VirtualColumnSharedDataType) -> DataType {
    match data_type {
        VirtualColumnSharedDataType::Boolean => DataType::Boolean,
        VirtualColumnSharedDataType::UInt64 => DataType::Number(NumberDataType::UInt64),
        VirtualColumnSharedDataType::Int64 => DataType::Number(NumberDataType::Int64),
        VirtualColumnSharedDataType::Float64 => DataType::Number(NumberDataType::Float64),
        VirtualColumnSharedDataType::String => DataType::String,
        VirtualColumnSharedDataType::Jsonb => DataType::Variant,
    }
}

#[cfg(test)]
mod tests {
    use databend_common_column::buffer::Buffer;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::ArrayColumn;
    use databend_common_expression::types::NumberColumn;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    #[test]
    fn test_materialize_shared_map_value() -> Result<()> {
        let map = Column::Map(Box::new(ArrayColumn::new(
            Column::Tuple(vec![
                Column::Number(NumberColumn::UInt32(Buffer::from(vec![1, 2, 2]))),
                Column::Number(NumberColumn::UInt64(Buffer::from(vec![10, 20, 30]))),
            ]),
            Buffer::from(vec![0, 2, 3]),
        )));

        let (value, data_type) = materialize_shared_map_value(
            map,
            DataType::Map(Box::new(DataType::Tuple(vec![
                DataType::Number(NumberDataType::UInt32),
                DataType::Number(NumberDataType::UInt64),
            ]))),
            VirtualColumnSharedDataType::UInt64,
            2,
            2,
        )?;
        assert_eq!(
            data_type,
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)))
        );
        let Value::Column(column) = value else {
            unreachable!()
        };
        assert_eq!(
            column.index(0),
            Some(ScalarRef::Number(NumberScalar::UInt64(20)))
        );
        assert_eq!(
            column.index(1),
            Some(ScalarRef::Number(NumberScalar::UInt64(30)))
        );

        let map = Column::Map(Box::new(ArrayColumn::new(
            Column::Tuple(vec![
                Column::Number(NumberColumn::UInt32(Buffer::from(vec![1, 2, 2]))),
                Column::Number(NumberColumn::UInt64(Buffer::from(vec![10, 20, 30]))),
            ]),
            Buffer::from(vec![0, 2, 3]),
        )));
        let (value, _) = materialize_shared_map_value(
            map,
            DataType::Map(Box::new(DataType::Tuple(vec![
                DataType::Number(NumberDataType::UInt32),
                DataType::Number(NumberDataType::UInt64),
            ]))),
            VirtualColumnSharedDataType::UInt64,
            3,
            2,
        )?;
        let Value::Column(column) = value else {
            unreachable!()
        };
        assert_eq!(column.index(0), Some(ScalarRef::Null));
        assert_eq!(column.index(1), Some(ScalarRef::Null));

        Ok(())
    }

    #[test]
    fn test_build_object_column_skips_null_values() -> Result<()> {
        let nullable_type = DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)));
        let mut builder = ColumnBuilder::with_capacity(&nullable_type, 2);
        builder.push(ScalarRef::Number(NumberScalar::UInt64(10)));
        builder.push(ScalarRef::Null);

        let (value, data_type) = build_object_column(
            &[("a", Value::Column(builder.build()))],
            &[],
            &FunctionContext::default(),
            2,
        )?;
        assert_eq!(data_type, DataType::Nullable(Box::new(DataType::Variant)));

        let Value::Column(column) = value else {
            unreachable!()
        };
        let Some(ScalarRef::Variant(bytes)) = column.index(0) else {
            unreachable!()
        };
        assert_eq!(RawJsonb::new(bytes).to_string(), r#"{"a":10}"#);
        assert_eq!(column.index(1), Some(ScalarRef::Null));

        Ok(())
    }

    #[test]
    fn test_build_object_column_reads_shared_group_directly() -> Result<()> {
        let map = Column::Map(Box::new(ArrayColumn::new(
            Column::Tuple(vec![
                Column::Number(NumberColumn::UInt32(Buffer::from(vec![1, 2, 2, 1]))),
                Column::Number(NumberColumn::UInt64(Buffer::from(vec![10, 20, 30, 40]))),
            ]),
            Buffer::from(vec![0, 2, 4]),
        )));
        let mut keys_by_index = HashMap::new();
        keys_by_index.insert(1, vec!["a"]);
        keys_by_index.insert(2, vec!["b"]);

        let group = SharedObjectGroup {
            column: map,
            keys_by_index,
        };
        let (value, data_type) =
            build_object_column(&[], &[group], &FunctionContext::default(), 2)?;
        assert_eq!(data_type, DataType::Nullable(Box::new(DataType::Variant)));

        let Value::Column(column) = value else {
            unreachable!()
        };
        let Some(ScalarRef::Variant(bytes)) = column.index(0) else {
            unreachable!()
        };
        assert_eq!(RawJsonb::new(bytes).to_string(), r#"{"a":10,"b":20}"#);
        let Some(ScalarRef::Variant(bytes)) = column.index(1) else {
            unreachable!()
        };
        assert_eq!(RawJsonb::new(bytes).to_string(), r#"{"a":40,"b":30}"#);

        Ok(())
    }

    #[test]
    fn test_build_object_column_keeps_coalesced_child_value() -> Result<()> {
        let variant_type = DataType::Nullable(Box::new(DataType::Variant));

        let mut missing_builder = ColumnBuilder::with_capacity(&variant_type, 1);
        missing_builder.push(ScalarRef::Null);

        let response = r#"{"msg":"ok","responseData":[2690218847],"statusCode":200}"#
            .parse::<OwnedJsonb>()
            .unwrap()
            .to_vec();
        let mut response_builder = ColumnBuilder::with_capacity(&variant_type, 1);
        response_builder.push(ScalarRef::Variant(response.as_slice()));

        let coalesced_response = materialize_coalesce_values(
            &[
                (Value::Column(missing_builder.build()), variant_type.clone()),
                (Value::Column(response_builder.build()), variant_type),
            ],
            &FunctionContext::default(),
            1,
        )?;

        let (value, _) = build_object_column(
            &[("response", coalesced_response.0)],
            &[],
            &FunctionContext::default(),
            1,
        )?;

        let Value::Column(column) = value else {
            unreachable!()
        };
        let Some(ScalarRef::Variant(bytes)) = column.index(0) else {
            unreachable!()
        };
        assert_eq!(
            RawJsonb::new(bytes).to_string(),
            r#"{"response":{"msg":"ok","responseData":[2690218847],"statusCode":200}}"#
        );

        Ok(())
    }

    #[test]
    fn test_materialize_shared_map_values() -> Result<()> {
        let map = Column::Map(Box::new(ArrayColumn::new(
            Column::Tuple(vec![
                Column::Number(NumberColumn::UInt32(Buffer::from(vec![1, 2, 2, 1]))),
                Column::Number(NumberColumn::UInt64(Buffer::from(vec![10, 20, 30, 40]))),
            ]),
            Buffer::from(vec![0, 2, 4]),
        )));

        let values = materialize_shared_map_values(
            map,
            DataType::Map(Box::new(DataType::Tuple(vec![
                DataType::Number(NumberDataType::UInt32),
                DataType::Number(NumberDataType::UInt64),
            ]))),
            VirtualColumnSharedDataType::UInt64,
            &[(10, 1), (11, 2)],
            2,
        )?;
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].0, 10);
        assert_eq!(values[1].0, 11);

        let Value::Column(column) = &values[0].1 else {
            unreachable!()
        };
        assert_eq!(
            column.index(0),
            Some(ScalarRef::Number(NumberScalar::UInt64(10)))
        );
        assert_eq!(
            column.index(1),
            Some(ScalarRef::Number(NumberScalar::UInt64(40)))
        );

        let Value::Column(column) = &values[1].1 else {
            unreachable!()
        };
        assert_eq!(
            column.index(0),
            Some(ScalarRef::Number(NumberScalar::UInt64(20)))
        );
        assert_eq!(
            column.index(1),
            Some(ScalarRef::Number(NumberScalar::UInt64(30)))
        );

        Ok(())
    }
}
