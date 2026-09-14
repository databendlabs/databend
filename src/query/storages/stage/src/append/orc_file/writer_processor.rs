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

use std::io;
use std::io::Write;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

use arrow_array::Array;
use arrow_array::BinaryArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::StringArray;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::NumberDataType;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;
use orc_rust::ArrowWriter;
use orc_rust::ArrowWriterBuilder;

use crate::append::column_based::file_writer::ColumnarFileEncoder;
use crate::append::column_based::file_writer::ColumnarFileWriter;

pub struct OrcFileWriter;

struct SendableOrcWriter(ArrowWriter<SharedBuffer>);

// orc-rust's internal column encoder trait object does not declare Send, but the
// concrete encoders it builds are owned buffers and primitive encoders. Keep the
// unsafe boundary local to this wrapper instead of marking the whole processor.
unsafe impl Send for SendableOrcWriter {}

#[derive(Clone)]
struct SharedBuffer {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
        }
    }

    fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    fn take(&self) -> Vec<u8> {
        mem::take(&mut *self.inner.lock().unwrap())
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct OrcEncoder {
    arrow_schema: SchemaRef,
    target_file_size: Option<usize>,
    writer: Option<SendableOrcWriter>,
    buffer: SharedBuffer,
}

const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_ORC_BATCH_SIZE: usize = 1024;

fn map_orc_error(e: orc_rust::error::OrcError) -> ErrorCode {
    ErrorCode::BadBytes(format!("failed to write ORC data: {e}"))
}

fn validate_orc_schema(schema: &TableSchemaRef) -> Result<()> {
    for field in schema.fields() {
        if !is_orc_rust_writer_supported_type(field.data_type()) {
            return Err(ErrorCode::Unimplemented(format!(
                "ORC output does not support column '{}' with Databend data type {}",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

// Keep this in sync with the current orc-rust ArrowWriter write path, not the
// complete ORC format type system.
fn is_orc_rust_writer_supported_type(data_type: &TableDataType) -> bool {
    matches!(
        data_type.remove_nullable(),
        TableDataType::Boolean
            | TableDataType::String
            | TableDataType::Binary
            | TableDataType::Number(
                NumberDataType::Int8
                    | NumberDataType::Int16
                    | NumberDataType::Int32
                    | NumberDataType::Int64
                    | NumberDataType::Float32
                    | NumberDataType::Float64,
            )
    )
}

fn orc_compatible_arrow_schema(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = match field.data_type() {
                DataType::Utf8View => DataType::Utf8,
                DataType::BinaryView => DataType::Binary,
                data_type => data_type.clone(),
            };
            Arc::new(
                Field::new(field.name(), data_type, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
            )
        })
        .collect::<Vec<_>>();

    Schema::new(fields).with_metadata(schema.metadata().clone())
}

fn orc_compatible_record_batch(batch: RecordBatch, arrow_schema: SchemaRef) -> Result<RecordBatch> {
    if arrow_schema.fields().is_empty() {
        return Ok(RecordBatch::try_new_with_options(
            arrow_schema,
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
        )?);
    }

    let arrays = batch
        .columns()
        .iter()
        .zip(arrow_schema.fields())
        .map(
            |(array, field)| match (array.data_type(), field.data_type()) {
                (DataType::Utf8View, DataType::Utf8) => {
                    Arc::new(StringArray::from_iter(array.as_string_view().iter()))
                        as Arc<dyn Array>
                }
                (DataType::BinaryView, DataType::Binary) => {
                    Arc::new(BinaryArray::from_iter(array.as_binary_view().iter()))
                        as Arc<dyn Array>
                }
                _ => array.clone(),
            },
        )
        .collect::<Vec<_>>();

    Ok(RecordBatch::try_new(arrow_schema, arrays)?)
}

fn create_writer(
    arrow_schema: SchemaRef,
    target_file_size: Option<usize>,
) -> Result<(ArrowWriter<SharedBuffer>, SharedBuffer)> {
    let buf_size = match target_file_size {
        Some(n) if n < MAX_BUFFER_SIZE => n,
        _ => MAX_BUFFER_SIZE,
    };
    let stripe_byte_size = target_file_size.unwrap_or(MAX_BUFFER_SIZE).max(1);
    let buffer = SharedBuffer::with_capacity(buf_size);
    let writer = ArrowWriterBuilder::new(buffer.clone(), arrow_schema)
        .with_batch_size(DEFAULT_ORC_BATCH_SIZE)
        .with_stripe_byte_size(stripe_byte_size)
        .try_build()
        .map_err(map_orc_error)?;
    Ok((writer, buffer))
}

impl OrcEncoder {
    fn try_create(schema: TableSchemaRef, target_file_size: Option<usize>) -> Result<Self> {
        validate_orc_schema(&schema)?;
        let source_arrow_schema = Schema::from(schema.as_ref());
        let arrow_schema = Arc::new(orc_compatible_arrow_schema(&source_arrow_schema));
        let (writer, buffer) = create_writer(arrow_schema.clone(), target_file_size)?;

        Ok(OrcEncoder {
            arrow_schema,
            target_file_size,
            writer: Some(SendableOrcWriter(writer)),
            buffer,
        })
    }

    fn reinit_writer(&mut self) -> Result<()> {
        let (writer, buffer) = create_writer(self.arrow_schema.clone(), self.target_file_size)?;
        self.writer = Some(SendableOrcWriter(writer));
        self.buffer = buffer;
        Ok(())
    }
}

impl ColumnarFileEncoder for OrcEncoder {
    const NAME: &'static str = "OrcFileWriter";

    fn write(&mut self, block: DataBlock, schema: &TableSchemaRef) -> Result<()> {
        let batch = block.to_record_batch(schema)?;
        let batch = orc_compatible_record_batch(batch, self.arrow_schema.clone())?;
        self.writer
            .as_mut()
            .unwrap()
            .0
            .write(&batch)
            .map_err(map_orc_error)?;
        Ok(())
    }

    fn bytes_written(&self) -> usize {
        self.buffer.len()
    }

    fn flush_on_batch_end(&self) -> bool {
        true
    }

    fn finish(&mut self) -> Result<Vec<u8>> {
        let writer = self.writer.take().unwrap();
        writer.0.close().map_err(map_orc_error)?;
        let data = self.buffer.take();
        self.reinit_writer()?;
        Ok(data)
    }
}

impl OrcFileWriter {
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
        let encoder = OrcEncoder::try_create(schema.clone(), target_file_size)?;
        ColumnarFileWriter::try_create(
            input,
            output,
            info,
            schema,
            data_accessor,
            query_id,
            group_id,
            target_file_size,
            encoder,
        )
    }
}
