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

use std::sync::Arc;

use arrow_ipc::writer::FileWriter;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;

use crate::append::column_based::file_writer::ColumnarFileEncoder;
use crate::append::column_based::file_writer::ColumnarFileWriter;
use crate::read::arrow::ArrowIpcMode;

pub struct ArrowFileWriter;

enum ArrowIpcWriter {
    File(FileWriter<Vec<u8>>),
    Stream(StreamWriter<Vec<u8>>),
}

impl ArrowIpcWriter {
    fn create(mode: ArrowIpcMode, arrow_schema: &Schema) -> Result<Self> {
        match mode {
            ArrowIpcMode::File => Ok(Self::File(FileWriter::try_new(vec![], arrow_schema)?)),
            ArrowIpcMode::Stream => Ok(Self::Stream(StreamWriter::try_new(vec![], arrow_schema)?)),
        }
    }

    fn write(&mut self, batch: &arrow_array::RecordBatch) -> Result<()> {
        match self {
            Self::File(writer) => writer.write(batch)?,
            Self::Stream(writer) => writer.write(batch)?,
        }
        Ok(())
    }

    fn bytes_written(&self) -> usize {
        match self {
            Self::File(writer) => writer.get_ref().len(),
            Self::Stream(writer) => writer.get_ref().len(),
        }
    }

    fn finish(self) -> Result<Vec<u8>> {
        match self {
            Self::File(writer) => Ok(writer.into_inner()?),
            Self::Stream(writer) => Ok(writer.into_inner()?),
        }
    }
}

struct ArrowEncoder {
    arrow_schema: Arc<Schema>,
    mode: ArrowIpcMode,
    writer: Option<ArrowIpcWriter>,
}

impl ArrowEncoder {
    fn try_create(schema: TableSchemaRef, mode: ArrowIpcMode) -> Result<Self> {
        let arrow_schema = Arc::new(Schema::from(schema.as_ref()));
        let writer = ArrowIpcWriter::create(mode, &arrow_schema)?;
        Ok(Self {
            arrow_schema,
            mode,
            writer: Some(writer),
        })
    }

    fn reinit_writer(&mut self) -> Result<()> {
        self.writer = Some(ArrowIpcWriter::create(self.mode, &self.arrow_schema)?);
        Ok(())
    }
}

impl ColumnarFileEncoder for ArrowEncoder {
    const NAME: &'static str = "ArrowFileWriter";

    fn write(&mut self, block: DataBlock, schema: &TableSchemaRef) -> Result<()> {
        let batch = block.to_record_batch(schema)?;
        self.writer.as_mut().unwrap().write(&batch)?;
        Ok(())
    }

    fn bytes_written(&self) -> usize {
        self.writer.as_ref().unwrap().bytes_written()
    }

    fn finish(&mut self) -> Result<Vec<u8>> {
        let writer = self.writer.take().unwrap();
        let data = writer.finish()?;
        self.reinit_writer()?;
        Ok(data)
    }
}

impl ArrowFileWriter {
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
        mode: ArrowIpcMode,
    ) -> Result<ProcessorPtr> {
        let encoder = ArrowEncoder::try_create(schema.clone(), mode)?;
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
