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
use apache_avro::Decimal;
use apache_avro::Schema as AvroSchema;
use apache_avro::Writer as AvroWriter;
use apache_avro::types::Value as AvroValue;
use arrow_schema::Schema;
use async_trait::async_trait;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
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
use crate::append::column_based::block_batch::BlockBatch;
use crate::append::output::DataSummary;
use crate::append::partition::partition_from_block;
use crate::append::path::unload_path;

pub struct ColumnBasedFileWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,

    input_data: VecDeque<DataBlock>,
    file_blocks: VecDeque<DataBlock>,
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

impl ColumnBasedFileWriter {
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
        Ok(ProcessorPtr::create(Box::new(ColumnBasedFileWriter {
            input,
            output,
            info,
            schema,
            unload_output,
            unload_output_blocks: None,
            input_data: VecDeque::new(),
            file_blocks: VecDeque::new(),
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

    fn flush_file(&mut self) -> Result<()> {
        let blocks = mem::take(&mut self.file_blocks);
        let data = match &self.info.stage.file_format_params {
            FileFormatParams::Orc(_) => serialize_orc(blocks, self.schema.clone())?,
            FileFormatParams::Avro(params) => {
                serialize_avro(blocks, self.schema.clone(), params.compression)?
            }
            _ => unreachable!(),
        };
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
impl Processor for ColumnBasedFileWriter {
    fn name(&self) -> String {
        "ColumnBasedFileWriter".to_string()
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
            if self.target_file_size.is_none() {
                self.input_data.push_back(block);
            } else if block.get_meta().is_some() {
                let block_meta = block.get_owned_meta().unwrap();
                let block_batch = BlockBatch::downcast_from(block_meta).unwrap();
                for b in block_batch.blocks {
                    self.input_data.push_back(b);
                }
            } else {
                self.input_data.push_back(block);
            }

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

            self.input_bytes += block.memory_size();
            self.row_counts += block.num_rows();
            self.file_blocks.push_back(block);

            if let Some(target) = self.target_file_size {
                if self.input_bytes >= target {
                    self.flush_file()?;
                    return Ok(());
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
        Ok(())
    }
}

fn serialize_orc(blocks: VecDeque<DataBlock>, schema: TableSchemaRef) -> Result<Vec<u8>> {
    let arrow_schema = Arc::new(Schema::from(schema.as_ref()));
    let mut data = Vec::new();
    {
        let mut writer = orc_rust::ArrowWriterBuilder::new(&mut data, arrow_schema)
            .try_build()
            .map_err(|e| ErrorCode::BadBytes(format!("failed to create ORC writer: {e}")))?;
        for block in blocks {
            let batch = block.to_record_batch(&schema)?;
            writer
                .write(&batch)
                .map_err(|e| ErrorCode::BadBytes(format!("failed to write ORC data: {e}")))?;
        }
        writer
            .close()
            .map_err(|e| ErrorCode::BadBytes(format!("failed to finish ORC writer: {e}")))?;
    }
    Ok(data)
}

fn serialize_avro(
    blocks: VecDeque<DataBlock>,
    schema: TableSchemaRef,
    compression: StageFileCompression,
) -> Result<Vec<u8>> {
    let avro_schema = build_avro_schema(&schema)?;
    let codec = avro_codec(compression)?;
    let mut writer = AvroWriter::with_codec(&avro_schema, Vec::new(), codec);
    for block in blocks.iter() {
        for row in 0..block.num_rows() {
            writer
                .append(avro_record(block, &schema, row)?)
                .map_err(|e| ErrorCode::BadBytes(format!("failed to write Avro data: {e}")))?;
        }
    }
    writer
        .flush()
        .map_err(|e| ErrorCode::BadBytes(format!("failed to finish Avro writer: {e}")))?;
    writer
        .into_inner()
        .map_err(|e| ErrorCode::BadBytes(format!("failed to finish Avro writer: {e}")))
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

fn build_avro_schema(schema: &TableSchemaRef) -> Result<AvroSchema> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            json!({
                "name": field.name(),
                "type": avro_type(field.data_type(), field.name())
            })
        })
        .collect::<Vec<_>>();
    let schema = json!({
        "type": "record",
        "name": "databend_unload_record",
        "fields": fields,
    });
    AvroSchema::parse(&schema)
        .map_err(|e| ErrorCode::BadBytes(format!("failed to build Avro schema: {e}")))
}

fn avro_type(ty: &TableDataType, name: &str) -> serde_json::Value {
    match ty {
        TableDataType::Null => json!("null"),
        TableDataType::EmptyArray => json!({"type": "array", "items": "null"}),
        TableDataType::EmptyMap => json!({"type": "map", "values": "null"}),
        TableDataType::Boolean => json!("boolean"),
        TableDataType::Binary | TableDataType::Bitmap | TableDataType::Geometry => json!("bytes"),
        TableDataType::String
        | TableDataType::Variant
        | TableDataType::Geography
        | TableDataType::Opaque(_) => json!("string"),
        TableDataType::Number(num_ty) => match num_ty {
            NumberDataType::UInt8
            | NumberDataType::UInt16
            | NumberDataType::UInt32
            | NumberDataType::Int8
            | NumberDataType::Int16
            | NumberDataType::Int32 => json!("int"),
            NumberDataType::UInt64 | NumberDataType::Int64 => json!("long"),
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
        TableDataType::Timestamp | TableDataType::TimestampTz => {
            json!({"type": "long", "logicalType": "timestamp-micros"})
        }
        TableDataType::Date => json!({"type": "int", "logicalType": "date"}),
        TableDataType::Nullable(inner) => json!(["null", avro_type(inner, name)]),
        TableDataType::Array(inner) => json!({"type": "array", "items": avro_type(inner, name)}),
        TableDataType::Map(inner) => match inner.as_ref() {
            TableDataType::Tuple { fields_type, .. }
                if matches!(fields_type.first(), Some(TableDataType::String)) =>
            {
                json!({"type": "map", "values": avro_type(&fields_type[1], name)})
            }
            _ => json!({
                "type": "array",
                "items": avro_type(inner, name),
            }),
        },
        TableDataType::Tuple {
            fields_name,
            fields_type,
        } => {
            let fields = fields_name
                .iter()
                .zip(fields_type)
                .enumerate()
                .map(|(idx, (field_name, field_type))| {
                    let field_name = if field_name.is_empty() {
                        format!("field_{idx}")
                    } else {
                        field_name.clone()
                    };
                    json!({
                        "name": field_name,
                        "type": avro_type(field_type, name),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "type": "record",
                "name": format!("{name}_record"),
                "fields": fields,
            })
        }
        TableDataType::Interval | TableDataType::Vector(_) | TableDataType::StageLocation => {
            json!("string")
        }
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
        (ScalarRef::Binary(v), _) | (ScalarRef::Bitmap(v), _) | (ScalarRef::Geometry(v), _) => {
            AvroValue::Bytes(v.to_vec())
        }
        (ScalarRef::String(v), _) => AvroValue::String(v.to_string()),
        (ScalarRef::Variant(v), _) => AvroValue::String(RawJsonb::new(v).to_string()),
        (ScalarRef::Number(v), _) => number_to_avro(v)?,
        (ScalarRef::Decimal(v), _) => AvroValue::Decimal(Decimal::from(decimal_bytes(v))),
        (ScalarRef::Timestamp(v), _) => AvroValue::TimestampMicros(v),
        (ScalarRef::TimestampTz(v), _) => AvroValue::TimestampMicros(v.timestamp()),
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
            let fields = values
                .into_iter()
                .zip(fields_name.iter().zip(fields_type))
                .map(|(value, (name, ty))| Ok((name.clone(), scalar_to_avro(value, ty)?)))
                .collect::<Result<Vec<_>>>()?;
            AvroValue::Record(fields)
        }
        (ScalarRef::Interval(v), _) => AvroValue::String(format!("{v:?}")),
        (ScalarRef::Geography(v), _) => AvroValue::String(format!("{v:?}")),
        (ScalarRef::Vector(v), _) => AvroValue::String(format!("{v:?}")),
        (ScalarRef::Opaque(v), _) => AvroValue::String(format!("{v:?}")),
        (value, ty) => {
            return Err(ErrorCode::InvalidArgument(format!(
                "Unsupported value {value:?} for Avro type {}",
                ty.sql_name()
            )));
        }
    };
    Ok(value)
}

fn number_to_avro(value: NumberScalar) -> Result<AvroValue> {
    match value {
        NumberScalar::UInt8(v) => Ok(AvroValue::Int(v.into())),
        NumberScalar::UInt16(v) => Ok(AvroValue::Int(v.into())),
        NumberScalar::UInt32(v) => i32::try_from(v)
            .map(AvroValue::Int)
            .map_err(|_| ErrorCode::InvalidArgument("UInt32 value is too large for Avro int")),
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
    if !matches!(fields_type.first(), Some(TableDataType::String)) {
        return Err(ErrorCode::InvalidArgument(
            "Avro map output only supports String keys",
        ));
    }

    let value_ty = &fields_type[1];
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
        values.insert((*key).to_string(), scalar_to_avro(value.clone(), value_ty)?);
    }
    Ok(AvroValue::Map(values))
}
