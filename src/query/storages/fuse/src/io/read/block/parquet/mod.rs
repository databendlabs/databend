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

use std::collections::HashMap;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use databend_common_catalog::plan::Projection;
use databend_common_exception::ErrorCode;
use databend_common_exception::span::Span;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FilterVisitor;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::cast_scalar;
use databend_common_expression::infer_schema_type;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::types::DataType;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalDataType;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::number::NumberDataType;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;

mod adapter;
mod deserialize;
mod row_selection;

pub use adapter::RowGroupImplBuilder;
pub use deserialize::column_chunks_to_record_batch;
pub use row_selection::RowSelection;

use crate::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::io::read::block::block_reader_merge_io::DataItem;

impl BlockReader {
    pub fn deserialize_part(
        &self,
        part: &FuseBlockPartInfo,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
    ) -> databend_common_exception::Result<DataBlock> {
        self.deserialize_parquet_chunks(
            part.nums_rows,
            &part.columns_meta,
            column_chunks,
            &part.compression,
            &part.location,
            selection,
            part.columns_stat.as_ref(),
        )
    }

    pub fn deserialize_parquet_chunks(
        &self,
        num_rows: usize,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        compression: &Compression,
        block_path: &str,
        selection: Option<&RowSelection>,
        column_stats: Option<&HashMap<ColumnId, ColumnStatistics>>,
    ) -> databend_common_exception::Result<DataBlock> {
        let result_rows = selection.map(|s| s.selected_rows).unwrap_or(num_rows);
        // If projection is empty, return a DataBlock with the appropriate row count but no columns
        if self.projected_schema.fields.is_empty() {
            return Ok(DataBlock::empty_with_rows(result_rows));
        }

        if result_rows == 0 {
            return Ok(DataBlock::empty_with_schema(&self.data_schema()));
        }

        if column_chunks.is_empty() {
            return self.build_default_values_block(result_rows);
        }

        // Read schema may differ from table schema when old blocks have legacy types.
        // Column stats let us infer a safer schema without touching parquet metadata.
        let mut read_original_schema = match column_stats {
            Some(stats) => build_read_schema(self.original_schema.as_ref(), stats),
            None => self.original_schema.clone(),
        };
        let mut read_projected_schema =
            if read_original_schema.as_ref() == self.original_schema.as_ref() {
                self.projected_schema.clone()
            } else {
                match &self.projection {
                    Projection::Columns(indices) => {
                        TableSchemaRef::new(read_original_schema.project(indices))
                    }
                    Projection::InnerColumns(path_indices) => {
                        TableSchemaRef::new(read_original_schema.inner_project(path_indices))
                    }
                }
            };

        let has_selection = selection.is_some();
        let parquet_selection = selection.map(|s| s.selection.clone());
        // Table schema may be widened (e.g. i32 -> i64); fallback narrows read types
        // back to the legacy parquet encoding for old blocks.
        let fallback_schema = build_compatible_read_schema(read_original_schema.as_ref());
        let fallback_projected_schema = fallback_schema.as_ref().map(|schema| {
            if schema.as_ref() == self.original_schema.as_ref() {
                self.projected_schema.clone()
            } else {
                match &self.projection {
                    Projection::Columns(indices) => TableSchemaRef::new(schema.project(indices)),
                    Projection::InnerColumns(path_indices) => {
                        TableSchemaRef::new(schema.inner_project(path_indices))
                    }
                }
            }
        });
        let mut last_err: Option<ErrorCode> = None;
        let mut record_batch: Option<(TableSchemaRef, TableSchemaRef, RecordBatch)> = None;
        // Try read once with a given schema; on success, remember the schemas for casting.
        let try_read =
            |schema: &TableSchemaRef,
             projected: &TableSchemaRef,
             record_batch: &mut Option<(TableSchemaRef, TableSchemaRef, RecordBatch)>,
             last_err: &mut Option<ErrorCode>| {
                match column_chunks_to_record_batch(
                    schema,
                    num_rows,
                    &column_chunks,
                    compression,
                    parquet_selection.clone(),
                ) {
                    Ok(batch) => {
                        *record_batch = Some((schema.clone(), projected.clone(), batch));
                    }
                    Err(err) => {
                        *last_err = Some(err);
                    }
                }
            };

        try_read(
            &read_original_schema,
            &read_projected_schema,
            &mut record_batch,
            &mut last_err,
        );
        if record_batch.is_none() {
            if let (Some(schema), Some(projected)) = (&fallback_schema, &fallback_projected_schema)
            {
                try_read(schema, projected, &mut record_batch, &mut last_err);
            }
        }

        let Some((schema, projected, record_batch)) = record_batch else {
            return Err(last_err.unwrap());
        };
        read_original_schema = schema;
        read_projected_schema = projected;

        let mut entries = Vec::with_capacity(self.projected_schema.fields.len());
        let name_paths = column_name_paths(&self.projection, &read_original_schema);

        let array_cache = if self.put_cache && !has_selection {
            CacheManager::instance().get_table_data_array_cache()
        } else {
            None
        };

        for (((i, field), read_field), column_node) in self
            .projected_schema
            .fields
            .iter()
            .enumerate()
            .zip(read_projected_schema.fields.iter())
            .zip(self.project_column_nodes.iter())
        {
            let read_data_type: DataType = read_field.data_type().into();

            // NOTE, there is something tricky here:
            // - `column_chunks` always contains data of leaf columns
            // - here we may processing a nested type field
            // - But, even if the field being processed is a field with multiple leaf columns
            //    `column_chunks.get(&field.column_id)` will still return Some(DataItem::_)[^1],
            //    even if we are getting data from `column_chunks` using a non-leaf
            //    `column_id` of `projected_schema.fields`
            //
            //   [^1]: Except in the current block, there is no data stored for the
            //         corresponding field, and a default value has been declared for
            //         the corresponding field.
            //
            //  Yes, it is too obscure, we need to polish it later.

            let value = match column_chunks.get(&field.column_id) {
                Some(DataItem::RawData(_)) => {
                    // get the deserialized arrow array, which may be a nested array
                    let arrow_array = column_by_name(&record_batch, &name_paths[i]);
                    if !column_node.is_nested {
                        if let Some(cache) = &array_cache {
                            let meta = column_metas.get(&field.column_id).unwrap();
                            let (offset, len) = meta.offset_length();
                            let key =
                                TableDataCacheKey::new(block_path, field.column_id, offset, len);
                            let array_memory_size = arrow_array.get_array_memory_size();
                            cache.insert(key.into(), (arrow_array.clone(), array_memory_size));
                        }
                    }
                    Value::from_arrow_rs(arrow_array, &read_data_type)?
                }
                Some(DataItem::ColumnArray(cached)) => {
                    if column_node.is_nested {
                        // a defensive check, should never happen
                        return Err(ErrorCode::StorageOther(
                            "unexpected nested field: nested leaf field hits cached",
                        ));
                    }
                    let mut value = Value::from_arrow_rs(cached.0.clone(), &read_data_type)?;
                    if let Some(selection) = selection {
                        let mut filter_visitor = FilterVisitor::new(&selection.bitmap);
                        filter_visitor.visit_value(value)?;
                        value = filter_visitor.take_result().unwrap();
                    }
                    value
                }
                None => {
                    if read_projected_schema.as_ref() == self.projected_schema.as_ref() {
                        Value::Scalar(self.default_vals[i].clone())
                    } else {
                        let scalar = cast_scalar(
                            Span::None,
                            self.default_vals[i].clone(),
                            &read_data_type,
                            &BUILTIN_FUNCTIONS,
                        )
                        .map_err(|err| {
                            let msg = format!(
                                "fail to cast default value for column {} to {}",
                                field.name(),
                                read_data_type
                            );
                            err.add_message(msg)
                        })?;
                        Value::Scalar(scalar)
                    }
                }
            };
            entries.push(BlockEntry::new(value, || (read_data_type, result_rows)));
        }
        let read_block = DataBlock::new(entries, result_rows);
        if read_projected_schema.as_ref() == self.projected_schema.as_ref() {
            return Ok(read_block);
        }

        let from_schema = DataSchemaRef::new(DataSchema::from(read_projected_schema.as_ref()));
        let to_schema = DataSchemaRef::new(DataSchema::from(self.projected_schema.as_ref()));
        cast_data_block(
            read_block,
            from_schema,
            to_schema,
            &self.ctx.get_function_context()?,
        )
    }
}

fn column_by_name(record_batch: &RecordBatch, names: &[String]) -> ArrayRef {
    let mut array = record_batch.column_by_name(&names[0]).unwrap().clone();
    if names.len() > 1 {
        for name in &names[1..] {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array = struct_array.column_by_name(name).unwrap().clone();
        }
    }
    array
}

// This function assumes that projection is valid, isn't responsible for checking it.
fn column_name_paths(projection: &Projection, schema: &TableSchema) -> Vec<Vec<String>> {
    match projection {
        Projection::Columns(field_indices) => field_indices
            .iter()
            .map(|i| vec![schema.fields[*i].name().to_string()])
            .collect(),
        Projection::InnerColumns(path_indices) => {
            let mut name_paths = Vec::with_capacity(path_indices.len());
            for index_path in path_indices.values() {
                let mut name_path = Vec::with_capacity(index_path.len());
                let first_index = index_path[0];
                name_path.push(schema.fields[first_index].name().to_string());
                let mut idx = 1;
                let mut ty = schema.fields[first_index].data_type().clone();
                while idx < index_path.len() {
                    match ty.remove_nullable() {
                        TableDataType::Tuple {
                            fields_name,
                            fields_type,
                        } => {
                            let next_index = index_path[idx];
                            name_path.push(fields_name[next_index].clone());
                            ty = fields_type[next_index].clone();
                        }
                        _ => unreachable!(),
                    }
                    idx += 1;
                }
                name_paths.push(name_path);
            }
            name_paths
        }
    }
}

fn build_read_schema(
    schema: &TableSchema,
    column_stats: &HashMap<ColumnId, ColumnStatistics>,
) -> TableSchemaRef {
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            let mut new_field = field.clone();
            let leaf_ids = field.leaf_column_ids();
            let new_type = adjust_type_by_stats(&field.data_type, &leaf_ids, column_stats);
            new_field.data_type = new_type;
            new_field
        })
        .collect::<Vec<TableField>>();
    TableSchemaRef::new(TableSchema::new_from_column_ids(
        fields,
        schema.metadata.clone(),
        schema.next_column_id,
    ))
}

fn adjust_type_by_stats(
    data_type: &TableDataType,
    leaf_ids: &[ColumnId],
    column_stats: &HashMap<ColumnId, ColumnStatistics>,
) -> TableDataType {
    match data_type {
        TableDataType::Nullable(inner) => TableDataType::Nullable(Box::new(adjust_type_by_stats(
            inner,
            leaf_ids,
            column_stats,
        ))),
        TableDataType::Array(inner) => TableDataType::Array(Box::new(adjust_type_by_stats(
            inner,
            leaf_ids,
            column_stats,
        ))),
        TableDataType::Map(inner) => TableDataType::Map(Box::new(adjust_type_by_stats(
            inner,
            leaf_ids,
            column_stats,
        ))),
        TableDataType::Tuple {
            fields_name,
            fields_type,
        } => {
            let mut offset = 0;
            let mut new_fields = Vec::with_capacity(fields_type.len());
            for inner in fields_type {
                let count = inner.num_leaf_columns();
                let end = offset + count;
                let slice = &leaf_ids[offset..end];
                new_fields.push(adjust_type_by_stats(inner, slice, column_stats));
                offset = end;
            }
            TableDataType::Tuple {
                fields_name: fields_name.clone(),
                fields_type: new_fields,
            }
        }
        _ => {
            if leaf_ids.is_empty() {
                return data_type.clone();
            }
            let column_id = leaf_ids[0];
            let Some(stat) = column_stats.get(&column_id) else {
                return data_type.clone();
            };
            let stat_type = if !stat.max().is_null() {
                Some(stat.max().as_ref().infer_data_type())
            } else if !stat.min().is_null() {
                Some(stat.min().as_ref().infer_data_type())
            } else {
                None
            };
            let Some(stat_type) = stat_type else {
                return data_type.clone();
            };
            let Ok(stat_table_type) = infer_schema_type(&stat_type) else {
                return data_type.clone();
            };
            if &stat_table_type == data_type {
                data_type.clone()
            } else {
                stat_table_type
            }
        }
    }
}

fn build_compatible_read_schema(schema: &TableSchema) -> Option<TableSchemaRef> {
    // Build a schema that can read legacy parquet encodings for widened types.
    let mut changed = false;
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            let (data_type, updated) = compatible_read_type(&field.data_type);
            if updated {
                changed = true;
            }
            let mut new_field = field.clone();
            new_field.data_type = data_type;
            new_field
        })
        .collect::<Vec<TableField>>();
    if !changed {
        return None;
    }
    Some(TableSchemaRef::new(TableSchema::new_from_column_ids(
        fields,
        schema.metadata.clone(),
        schema.next_column_id,
    )))
}

fn compatible_read_type(data_type: &TableDataType) -> (TableDataType, bool) {
    // Map widened types to a parquet-compatible read type; caller decides if used.
    match data_type {
        TableDataType::Nullable(inner) => {
            let (inner_type, changed) = compatible_read_type(inner);
            (TableDataType::Nullable(Box::new(inner_type)), changed)
        }
        TableDataType::Array(inner) => {
            let (inner_type, changed) = compatible_read_type(inner);
            (TableDataType::Array(Box::new(inner_type)), changed)
        }
        TableDataType::Map(inner) => {
            let (inner_type, changed) = compatible_read_type(inner);
            (TableDataType::Map(Box::new(inner_type)), changed)
        }
        TableDataType::Tuple {
            fields_name,
            fields_type,
        } => {
            let mut changed = false;
            let new_fields = fields_type
                .iter()
                .map(|inner| {
                    let (inner_type, inner_changed) = compatible_read_type(inner);
                    if inner_changed {
                        changed = true;
                    }
                    inner_type
                })
                .collect();
            (
                TableDataType::Tuple {
                    fields_name: fields_name.clone(),
                    fields_type: new_fields,
                },
                changed,
            )
        }
        TableDataType::Number(num) => match num {
            NumberDataType::Int64 => (TableDataType::Number(NumberDataType::Int32), true),
            NumberDataType::Int32 => (TableDataType::Number(NumberDataType::Int16), true),
            NumberDataType::Int16 => (TableDataType::Number(NumberDataType::Int8), true),
            NumberDataType::UInt64 => (TableDataType::Number(NumberDataType::UInt32), true),
            NumberDataType::UInt32 => (TableDataType::Number(NumberDataType::UInt16), true),
            NumberDataType::UInt16 => (TableDataType::Number(NumberDataType::UInt8), true),
            NumberDataType::Float64 => (TableDataType::Number(NumberDataType::Float32), true),
            _ => (data_type.clone(), false),
        },
        TableDataType::Decimal(decimal) => match compatible_decimal_read_type(decimal) {
            Some(decimal) => (TableDataType::Decimal(decimal), true),
            None => (data_type.clone(), false),
        },
        _ => (data_type.clone(), false),
    }
}

fn compatible_decimal_read_type(decimal: &DecimalDataType) -> Option<DecimalDataType> {
    let size = decimal.size();
    let scale = size.scale();
    let precision = size.precision();
    match decimal {
        DecimalDataType::Decimal128(_) => {
            let max_precision = <i64 as Decimal>::MAX_PRECISION;
            if scale > max_precision {
                None
            } else {
                // Parquet decimal may be encoded with a wider logical type.
                // Decimal64 can safely represent up to i64::MAX_PRECISION digits.
                let target_precision = precision.min(max_precision).max(scale);
                Some(DecimalDataType::Decimal64(DecimalSize::new_unchecked(
                    target_precision,
                    scale,
                )))
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::TableDataType;
    use databend_common_expression::types::NumberDataType;

    use super::compatible_read_type;

    #[test]
    fn test_compatible_read_type_widening_numbers() {
        let cases = [
            (NumberDataType::Int64, NumberDataType::Int32),
            (NumberDataType::Int32, NumberDataType::Int16),
            (NumberDataType::Int16, NumberDataType::Int8),
            (NumberDataType::UInt64, NumberDataType::UInt32),
            (NumberDataType::UInt32, NumberDataType::UInt16),
            (NumberDataType::UInt16, NumberDataType::UInt8),
            (NumberDataType::Float64, NumberDataType::Float32),
        ];

        for (from, to) in cases {
            let (ty, changed) = compatible_read_type(&TableDataType::Number(from));
            assert_eq!(ty, TableDataType::Number(to));
            assert!(changed);
        }

        let (ty, changed) = compatible_read_type(&TableDataType::Number(NumberDataType::Int8));
        assert_eq!(ty, TableDataType::Number(NumberDataType::Int8));
        assert!(!changed);

        let (ty, changed) = compatible_read_type(&TableDataType::Number(NumberDataType::UInt8));
        assert_eq!(ty, TableDataType::Number(NumberDataType::UInt8));
        assert!(!changed);

        let (ty, changed) = compatible_read_type(&TableDataType::Number(NumberDataType::Float32));
        assert_eq!(ty, TableDataType::Number(NumberDataType::Float32));
        assert!(!changed);
    }
}

fn cast_data_block(
    data_block: DataBlock,
    from_schema: DataSchemaRef,
    to_schema: DataSchemaRef,
    func_ctx: &FunctionContext,
) -> databend_common_exception::Result<DataBlock> {
    let exprs = from_schema
        .fields()
        .iter()
        .zip(to_schema.fields().iter().enumerate())
        .map(|(from, (index, to))| {
            let expr = ColumnRef {
                span: None,
                id: index,
                data_type: from.data_type().clone(),
                display_name: from.name().clone(),
            };
            check_cast(
                None,
                false,
                Expr::from(expr),
                to.data_type(),
                &BUILTIN_FUNCTIONS,
            )
        })
        .collect::<databend_common_exception::Result<Vec<_>>>()?;

    let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);
    let mut entries = Vec::with_capacity(exprs.len());
    for (i, (field, expr)) in to_schema.fields().iter().zip(exprs.iter()).enumerate() {
        let value = evaluator.run(expr).map_err(|err| {
            let msg = format!(
                "fail to auto cast column {} ({}) to column {} ({})",
                from_schema.fields()[i].name(),
                from_schema.fields()[i].data_type(),
                field.name(),
                field.data_type(),
            );
            err.add_message(msg)
        })?;
        let entry = BlockEntry::new(value, || (field.data_type().clone(), data_block.num_rows()));
        entries.push(entry);
    }
    Ok(DataBlock::new(entries, data_block.num_rows()))
}
