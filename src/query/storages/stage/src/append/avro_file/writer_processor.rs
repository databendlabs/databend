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

use std::any::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use apache_avro::Codec;
use apache_avro::Days;
use apache_avro::Decimal;
use apache_avro::Duration;
use apache_avro::Millis;
use apache_avro::Months;
use apache_avro::Schema as AvroSchema;
use apache_avro::to_avro_datum;
use apache_avro::types::Value as AvroValue;
use async_trait::async_trait;
use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::VectorDataType;
use databend_common_expression::types::VectorScalarRef;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_storage::ensure_no_stage_path_traversal;
use databend_storages_common_stage::CopyIntoLocationInfo;
use jsonb::RawJsonb;
use num_bigint::BigInt;
use opendal::Operator;
use serde_json::json;

use crate::append::UnloadOutput;
use crate::append::output::DataSummary;
use crate::append::partition::partition_from_block;
use crate::append::path::unload_path;
use crate::avro_utils::avro_tuple_field_names;

const AVRO_WRITE_CHUNK_ROWS: usize = 1024;
const AVRO_OBJECT_HEADER: &[u8] = b"Obj\x01";
const DEFAULT_AVRO_BLOCK_SIZE: usize = 16 * 1024;

pub struct AvroFileWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,

    input_data: VecDeque<DataBlock>,
    file_data: Vec<u8>,
    block_data: Vec<u8>,
    block_rows: usize,
    avro_schema: AvroSchema,
    codec: Codec,
    sync_marker: [u8; 16],
    input_bytes: usize,
    row_counts: usize,

    file_to_write: Option<(Vec<u8>, DataSummary, Option<Arc<str>>)>,
    data_accessor: Operator,

    unload_output: UnloadOutput,
    unload_output_blocks: Option<VecDeque<DataBlock>>,

    query_id: String,
    group_id: usize,
    batch_id: usize,

    target_file_size: Option<usize>,
    current_partition: Option<Option<Arc<str>>>,
}

impl AvroFileWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        info: CopyIntoLocationInfo,
        schema: TableSchemaRef,
        data_accessor: Operator,
        query_id: String,
        group_id: usize,
        target_file_size: Option<usize>,
    ) -> Result<ProcessorPtr> {
        let unload_output = UnloadOutput::create(info.options.detailed_output);
        let avro_schema = build_avro_schema(&schema)?;
        let codec = if let FileFormatParams::Avro(params) = &info.stage.file_format_params {
            avro_codec(params.compression)?
        } else {
            unreachable!()
        };
        let sync_marker = avro_sync_marker();
        Ok(ProcessorPtr::create(Box::new(AvroFileWriter {
            input,
            output,
            info,
            schema,
            unload_output,
            unload_output_blocks: None,
            input_data: VecDeque::new(),
            file_data: Vec::new(),
            block_data: Vec::new(),
            block_rows: 0,
            avro_schema,
            codec,
            sync_marker,
            input_bytes: 0,
            row_counts: 0,
            file_to_write: None,
            data_accessor,
            query_id,
            group_id,
            batch_id: 0,
            target_file_size,
            current_partition: None,
        })))
    }

    fn append_block(&mut self, block: &DataBlock) -> Result<()> {
        for row in 0..block.num_rows() {
            let value = avro_record(block, &self.schema, row)?;
            let row_data = to_avro_datum(&self.avro_schema, value)
                .map_err(|e| ErrorCode::BadBytes(format!("failed to write Avro data: {e}")))?;
            self.block_data.extend(row_data);
            self.block_rows += 1;

            if self.block_data.len() >= DEFAULT_AVRO_BLOCK_SIZE {
                self.flush_avro_block()?;
            }
        }
        Ok(())
    }

    fn ensure_header(&mut self) -> Result<()> {
        if !self.file_data.is_empty() {
            return Ok(());
        }

        let schema_bytes = serde_json::to_string(&self.avro_schema)
            .map_err(|e| ErrorCode::BadBytes(format!("failed to build Avro header: {e}")))?
            .into_bytes();
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert("avro.schema".to_string(), AvroValue::Bytes(schema_bytes));
        metadata.insert("avro.codec".to_string(), self.codec.into());

        self.file_data.extend_from_slice(AVRO_OBJECT_HEADER);
        self.file_data.extend(
            to_avro_datum(
                &AvroSchema::map(AvroSchema::Bytes),
                AvroValue::Map(metadata),
            )
            .map_err(|e| ErrorCode::BadBytes(format!("failed to build Avro header: {e}")))?,
        );
        self.file_data.extend_from_slice(&self.sync_marker);
        Ok(())
    }

    fn flush_avro_block(&mut self) -> Result<()> {
        if self.block_rows == 0 {
            return Ok(());
        }

        self.ensure_header()?;
        self.codec
            .compress(&mut self.block_data)
            .map_err(|e| ErrorCode::BadBytes(format!("failed to compress Avro data: {e}")))?;

        self.file_data.extend(
            to_avro_datum(&AvroSchema::Long, AvroValue::Long(self.block_rows as i64))
                .map_err(|e| ErrorCode::BadBytes(format!("failed to write Avro data: {e}")))?,
        );
        self.file_data.extend(
            to_avro_datum(
                &AvroSchema::Long,
                AvroValue::Long(self.block_data.len() as i64),
            )
            .map_err(|e| ErrorCode::BadBytes(format!("failed to write Avro data: {e}")))?,
        );
        self.file_data.extend_from_slice(&self.block_data);
        self.file_data.extend_from_slice(&self.sync_marker);
        self.block_data.clear();
        self.block_rows = 0;
        Ok(())
    }

    fn flush_file(&mut self) -> Result<()> {
        self.flush_avro_block()?;
        let data = mem::take(&mut self.file_data);
        let output_bytes = data.len();
        self.file_to_write = Some((
            data,
            DataSummary {
                row_counts: self.row_counts,
                input_bytes: self.input_bytes,
                output_bytes,
            },
            self.current_partition.clone().flatten(),
        ));
        self.row_counts = 0;
        self.input_bytes = 0;
        self.current_partition = None;
        Ok(())
    }
}

#[async_trait]
impl Processor for AvroFileWriter {
    fn name(&self) -> String {
        "AvroFileWriter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            Ok(Event::Finished)
        } else if self.file_to_write.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Async)
        } else if !self.input_data.is_empty() {
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else if self.input.is_finished() {
            if self.row_counts > 0 {
                return Ok(Event::Sync);
            }
            if self.unload_output.is_empty() {
                self.output.finish();
                return Ok(Event::Finished);
            }
            if self.unload_output_blocks.is_none() {
                self.unload_output_blocks = Some(self.unload_output.to_block_partial().into());
            }
            if self.output.can_push() {
                if let Some(block) = self.unload_output_blocks.as_mut().unwrap().pop_front() {
                    self.output.push_data(Ok(block));
                    Ok(Event::NeedConsume)
                } else {
                    self.output.finish();
                    Ok(Event::Finished)
                }
            } else {
                Ok(Event::NeedConsume)
            }
        } else if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;
            self.input_data.push_back(block);
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        while let Some(block) = self.input_data.pop_front() {
            let partition = partition_from_block(&block);
            if self.current_partition.as_ref() != Some(&partition) {
                if self.row_counts > 0 {
                    self.input_data.push_front(block);
                    self.flush_file()?;
                    return Ok(());
                }
                self.current_partition = Some(partition.clone());
            }

            let rows = block.num_rows();
            let mut start = 0;
            while start < rows {
                let end = (start + AVRO_WRITE_CHUNK_ROWS).min(rows);
                let chunk = block.slice(start..end);
                self.input_bytes += chunk.memory_size();
                self.row_counts += chunk.num_rows();
                self.append_block(&chunk)?;
                start = end;

                if let Some(target) = self.target_file_size {
                    self.flush_avro_block()?;
                    if self.file_data.len() >= target {
                        if start < rows {
                            self.input_data.push_front(block.slice(start..rows));
                        }
                        self.flush_file()?;
                        return Ok(());
                    }
                }
            }
        }

        if self.input.is_finished() && self.row_counts > 0 {
            self.flush_file()?;
            return Ok(());
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        assert!(self.file_to_write.is_some());
        let (data, summary, partition) = mem::take(&mut self.file_to_write).unwrap();
        let path = unload_path(
            &self.info,
            &self.query_id,
            self.group_id,
            self.batch_id,
            None,
            partition.as_deref(),
        );
        if !self.info.allow_path_traversal {
            ensure_no_stage_path_traversal(&path)?;
        }
        self.unload_output.add_file(&path, summary);
        self.data_accessor.write(&path, data).await?;
        self.batch_id += 1;
        self.sync_marker = avro_sync_marker();
        Ok(())
    }
}

fn avro_codec(compression: StageFileCompression) -> Result<Codec> {
    match compression {
        StageFileCompression::None | StageFileCompression::Auto => Ok(Codec::Null),
        StageFileCompression::Deflate | StageFileCompression::RawDeflate => Ok(Codec::Deflate),
        StageFileCompression::Snappy => Ok(Codec::Snappy),
        StageFileCompression::Zstd => Ok(Codec::Zstandard),
        StageFileCompression::Bz2 => Ok(Codec::Bzip2),
        StageFileCompression::Xz => Ok(Codec::Xz),
        _ => Err(ErrorCode::InvalidArgument(format!(
            "Unsupported Avro compression: {compression}"
        ))),
    }
}

fn avro_sync_marker() -> [u8; 16] {
    *uuid::Uuid::new_v4().as_bytes()
}

fn build_avro_schema(schema: &TableSchemaRef) -> Result<AvroSchema> {
    let mut builder = AvroSchemaBuilder::default();
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(json!({
                "name": field.name(),
                "type": builder.avro_type(field.data_type())?
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = json!({
        "type": "record",
        "name": "databend_unload_record",
        "fields": fields,
    });
    AvroSchema::parse(&schema)
        .map_err(|e| ErrorCode::BadBytes(format!("failed to build Avro schema: {e}")))
}

#[derive(Default)]
struct AvroSchemaBuilder {
    record_id: usize,
}

impl AvroSchemaBuilder {
    fn avro_type(&mut self, ty: &TableDataType) -> Result<serde_json::Value> {
        let value = match ty {
            TableDataType::Null => json!("null"),
            TableDataType::EmptyArray => json!({"type": "array", "items": "null"}),
            TableDataType::EmptyMap => json!({"type": "map", "values": "null"}),
            TableDataType::Boolean => json!("boolean"),
            TableDataType::Binary => json!("bytes"),
            TableDataType::String | TableDataType::Variant => json!("string"),
            TableDataType::Bitmap
            | TableDataType::Geometry
            | TableDataType::Geography
            | TableDataType::Opaque(_)
            | TableDataType::TimestampTz => {
                // Keep Avro unload aligned with Avro load: these logical/internal
                // Databend types are not supported by the Avro reader yet.
                return Err(ErrorCode::InvalidArgument(format!(
                    "Avro output does not support {}",
                    ty.sql_name()
                )));
            }
            TableDataType::Number(num_ty) => match num_ty {
                NumberDataType::UInt8
                | NumberDataType::UInt16
                | NumberDataType::Int8
                | NumberDataType::Int16
                | NumberDataType::Int32 => json!("int"),
                NumberDataType::UInt32 | NumberDataType::UInt64 | NumberDataType::Int64 => {
                    json!("long")
                }
                NumberDataType::Float32 => json!("float"),
                NumberDataType::Float64 => json!("double"),
            },
            TableDataType::Decimal(decimal_ty) => {
                let size = match decimal_ty {
                    DecimalDataType::Decimal64(size)
                    | DecimalDataType::Decimal128(size)
                    | DecimalDataType::Decimal256(size) => size,
                };
                json!({
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": size.precision(),
                    "scale": size.scale(),
                })
            }
            TableDataType::Timestamp => json!({"type": "long", "logicalType": "timestamp-micros"}),
            TableDataType::Date => json!({"type": "int", "logicalType": "date"}),
            TableDataType::Nullable(inner) => json!(["null", self.avro_type(inner)?]),
            TableDataType::Array(inner) => {
                json!({"type": "array", "items": self.avro_type(inner)?})
            }
            TableDataType::Map(inner) => match inner.as_ref() {
                TableDataType::Tuple { fields_type, .. }
                    if matches!(fields_type.first(), Some(TableDataType::String)) =>
                {
                    json!({"type": "map", "values": self.avro_type(&fields_type[1])?})
                }
                _ => {
                    return Err(ErrorCode::InvalidArgument(
                        "Avro map output only supports String keys",
                    ));
                }
            },
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => self.avro_record_type(fields_name, fields_type)?,
            TableDataType::Interval => {
                json!({"type": {"type": "fixed", "name": "duration", "size": 12}, "logicalType": "duration"})
            }
            TableDataType::Vector(vector_ty) => {
                json!({"type": "array", "items": avro_vector_item_type(vector_ty)})
            }
            TableDataType::StageLocation => json!("string"),
        };
        Ok(value)
    }

    fn avro_record_type(
        &mut self,
        fields_name: &[String],
        fields_type: &[TableDataType],
    ) -> Result<serde_json::Value> {
        let record_name = format!("databend_tuple_record_{}", self.record_id);
        self.record_id += 1;
        let avro_fields_name = avro_tuple_field_names(fields_name);
        let fields = fields_name
            .iter()
            .zip(avro_fields_name)
            .zip(fields_type)
            .map(|((_, avro_field_name), field_type)| {
                Ok(json!({
                    "name": avro_field_name,
                    "type": self.avro_type(field_type)?,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(json!({
            "type": "record",
            "name": record_name,
            "fields": fields,
        }))
    }
}

fn avro_record(block: &DataBlock, schema: &TableSchemaRef, row: usize) -> Result<AvroValue> {
    let mut record = Vec::with_capacity(schema.num_fields());
    for (field, entry) in schema.fields().iter().zip(block.columns()) {
        let value = entry
            .index(row)
            .ok_or_else(|| ErrorCode::Internal("invalid row index while writing Avro"))?;
        record.push((
            field.name().clone(),
            scalar_to_avro(value, field.data_type())?,
        ));
    }
    Ok(AvroValue::Record(record))
}

fn scalar_to_avro(value: ScalarRef, ty: &TableDataType) -> Result<AvroValue> {
    if let TableDataType::Nullable(inner) = ty {
        return if matches!(value, ScalarRef::Null) {
            Ok(AvroValue::Union(0, Box::new(AvroValue::Null)))
        } else {
            Ok(AvroValue::Union(1, Box::new(scalar_to_avro(value, inner)?)))
        };
    }

    let value = match (value, ty) {
        (ScalarRef::Null, _) => AvroValue::Null,
        (ScalarRef::EmptyArray, TableDataType::EmptyArray) => AvroValue::Array(vec![]),
        (ScalarRef::EmptyMap, TableDataType::EmptyMap) => AvroValue::Map(HashMap::new()),
        (ScalarRef::Boolean(v), _) => AvroValue::Boolean(v),
        (ScalarRef::Binary(v), _) => AvroValue::Bytes(v.to_vec()),
        (ScalarRef::String(v), _) => AvroValue::String(v.to_string()),
        (ScalarRef::Variant(v), _) => AvroValue::String(RawJsonb::new(v).to_string()),
        (ScalarRef::Number(v), _) => number_to_avro(v)?,
        (ScalarRef::Decimal(v), _) => AvroValue::Decimal(Decimal::from(decimal_bytes(v))),
        (ScalarRef::Timestamp(v), _) => AvroValue::TimestampMicros(v),
        (ScalarRef::Date(v), _) => AvroValue::Date(v),
        (ScalarRef::Array(col), TableDataType::Array(inner)) => {
            let mut values = Vec::with_capacity(col.len());
            for idx in 0..col.len() {
                let item = col
                    .index(idx)
                    .ok_or_else(|| ErrorCode::Internal("invalid array index while writing Avro"))?;
                values.push(scalar_to_avro(item, inner)?);
            }
            AvroValue::Array(values)
        }
        (ScalarRef::Map(col), TableDataType::Map(inner)) => map_to_avro(col, inner)?,
        (
            ScalarRef::Tuple(values),
            TableDataType::Tuple {
                fields_name,
                fields_type,
            },
        ) => {
            let avro_fields_name = avro_tuple_field_names(fields_name);
            let fields = values
                .into_iter()
                .zip(avro_fields_name.into_iter().zip(fields_type))
                .map(|(value, (name, ty))| Ok((name, scalar_to_avro(value, ty)?)))
                .collect::<Result<Vec<_>>>()?;
            AvroValue::Record(fields)
        }
        (ScalarRef::Interval(v), _) => interval_to_avro(v)?,
        (ScalarRef::Vector(v), _) => vector_to_avro(v),
        (value, ty) => {
            return Err(ErrorCode::InvalidArgument(format!(
                "Unsupported value {value:?} for Avro type {}",
                ty.sql_name()
            )));
        }
    };
    Ok(value)
}

fn avro_vector_item_type(vector_ty: &VectorDataType) -> serde_json::Value {
    match vector_ty {
        VectorDataType::Int8(_) => json!("int"),
        VectorDataType::Float32(_) => json!("float"),
    }
}

fn interval_to_avro(value: months_days_micros) -> Result<AvroValue> {
    let months = u32::try_from(value.months()).map_err(|_| {
        ErrorCode::InvalidArgument("Avro duration output does not support negative months")
    })?;
    let days = u32::try_from(value.days()).map_err(|_| {
        ErrorCode::InvalidArgument("Avro duration output does not support negative days")
    })?;
    let millis = u32::try_from(value.microseconds() / 1000).map_err(|_| {
        ErrorCode::InvalidArgument(
            "Avro duration output only supports milliseconds in the range 0..=u32::MAX",
        )
    })?;

    Ok(AvroValue::Duration(Duration::new(
        Months::new(months),
        Days::new(days),
        Millis::new(millis),
    )))
}

fn vector_to_avro(value: VectorScalarRef) -> AvroValue {
    match value {
        VectorScalarRef::Int8(values) => {
            AvroValue::Array(values.iter().map(|v| AvroValue::Int((*v).into())).collect())
        }
        VectorScalarRef::Float32(values) => {
            AvroValue::Array(values.iter().map(|v| AvroValue::Float(v.0)).collect())
        }
    }
}

fn number_to_avro(value: NumberScalar) -> Result<AvroValue> {
    match value {
        NumberScalar::UInt8(v) => Ok(AvroValue::Int(v.into())),
        NumberScalar::UInt16(v) => Ok(AvroValue::Int(v.into())),
        NumberScalar::UInt32(v) => Ok(AvroValue::Long(v.into())),
        NumberScalar::UInt64(v) => i64::try_from(v)
            .map(AvroValue::Long)
            .map_err(|_| ErrorCode::InvalidArgument("UInt64 value is too large for Avro long")),
        NumberScalar::Int8(v) => Ok(AvroValue::Int(v.into())),
        NumberScalar::Int16(v) => Ok(AvroValue::Int(v.into())),
        NumberScalar::Int32(v) => Ok(AvroValue::Int(v)),
        NumberScalar::Int64(v) => Ok(AvroValue::Long(v)),
        NumberScalar::Float32(v) => Ok(AvroValue::Float(v.0)),
        NumberScalar::Float64(v) => Ok(AvroValue::Double(v.0)),
    }
}

fn decimal_bytes(value: DecimalScalar) -> Vec<u8> {
    match value {
        DecimalScalar::Decimal64(v, _) => BigInt::from(v).to_signed_bytes_be(),
        DecimalScalar::Decimal128(v, _) => BigInt::from(v).to_signed_bytes_be(),
        DecimalScalar::Decimal256(v, _) => {
            BigInt::from_signed_bytes_le(&v.to_le_bytes()).to_signed_bytes_be()
        }
    }
}

fn map_to_avro(col: databend_common_expression::Column, ty: &TableDataType) -> Result<AvroValue> {
    let TableDataType::Tuple { fields_type, .. } = ty else {
        return Err(ErrorCode::InvalidArgument(
            "Invalid Databend map type for Avro",
        ));
    };

    let mut values = HashMap::with_capacity(col.len());
    for idx in 0..col.len() {
        let entry = col
            .index(idx)
            .ok_or_else(|| ErrorCode::Internal("invalid map index while writing Avro"))?;
        let ScalarRef::Tuple(kv) = entry else {
            return Err(ErrorCode::Internal("invalid map entry while writing Avro"));
        };
        let [ScalarRef::String(key), value] = &kv[..] else {
            return Err(ErrorCode::InvalidArgument(
                "Avro map output only supports String keys",
            ));
        };
        values.insert(
            (*key).to_string(),
            scalar_to_avro(value.clone(), &fields_type[1])?,
        );
    }
    Ok(AvroValue::Map(values))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use apache_avro::Schema;
    use databend_common_column::types::months_days_micros;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::F32;
    use databend_common_expression::types::VectorDataType;
    use databend_common_expression::types::VectorScalarRef;

    use super::*;

    #[test]
    fn test_build_avro_schema_for_special_types() {
        let schema = Arc::new(TableSchema::new(vec![
            TableField::new("i", TableDataType::Interval),
            TableField::new("i2", TableDataType::Interval),
            TableField::new("v", TableDataType::Vector(VectorDataType::Float32(3))),
            TableField::new("d", TableDataType::Tuple {
                fields_name: vec!["1".to_string(), "2".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int32),
                    TableDataType::String,
                ],
            }),
        ]));

        let avro_schema = build_avro_schema(&schema).unwrap();
        let Schema::Record(record) = avro_schema else {
            panic!("expected Avro record schema");
        };
        assert!(matches!(&record.fields[0].schema, Schema::Duration));
        assert!(matches!(&record.fields[1].schema, Schema::Duration));
        let Schema::Array(array) = &record.fields[2].schema else {
            panic!("expected Avro array schema");
        };
        assert!(matches!(array.items.as_ref(), Schema::Float));
        let Schema::Record(tuple_record) = &record.fields[3].schema else {
            panic!("expected Avro tuple record schema");
        };
        assert_eq!(tuple_record.fields[0].name, "field_0");
        assert_eq!(tuple_record.fields[1].name, "field_1");
    }

    #[test]
    fn test_interval_to_avro_duration() {
        let value = months_days_micros::new(1, 2, 3_000);
        let avro_value = scalar_to_avro(ScalarRef::Interval(value), &TableDataType::Interval)
            .expect("interval should convert to Avro duration");
        let AvroValue::Duration(duration) = avro_value else {
            panic!("expected Avro duration");
        };
        assert_eq!(u32::from(duration.months()), 1);
        assert_eq!(u32::from(duration.days()), 2);
        assert_eq!(u32::from(duration.millis()), 3);

        let negative = months_days_micros::new(-1, 0, 0);
        assert!(scalar_to_avro(ScalarRef::Interval(negative), &TableDataType::Interval).is_err());
    }

    #[test]
    fn test_vector_to_avro_array() {
        let values = [F32::from(1.5), F32::from(2.5), F32::from(3.5)];
        let avro_value = scalar_to_avro(
            ScalarRef::Vector(VectorScalarRef::Float32(&values)),
            &TableDataType::Vector(VectorDataType::Float32(3)),
        )
        .expect("vector should convert to Avro array");

        assert_eq!(
            avro_value,
            AvroValue::Array(vec![
                AvroValue::Float(1.5),
                AvroValue::Float(2.5),
                AvroValue::Float(3.5),
            ])
        );
    }
}
