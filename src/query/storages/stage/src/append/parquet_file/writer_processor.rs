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

use std::mem;
use std::sync::Arc;

use arrow_schema::DataType;
use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::Encoding;
use parquet::basic::ZstdLevel;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;
use parquet::schema::types::ColumnPath;

use crate::append::column_based::file_writer::ColumnarFileEncoder;
use crate::append::column_based::file_writer::ColumnarFileWriter;

pub struct ParquetFileWriter;

struct ParquetEncoder {
    arrow_schema: Arc<Schema>,
    compression: Compression,
    create_by: String,
    target_file_size: Option<usize>,
    writer: ArrowWriter<Vec<u8>>,
}

const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;
// this is number of rows, not size
const MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;

fn create_writer(
    arrow_schema: Arc<Schema>,
    target_file_size: Option<usize>,
    compression: Compression,
    create_by: String,
) -> Result<ArrowWriter<Vec<u8>>> {
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(compression)
        .set_created_by(create_by)
        .set_max_row_group_row_count(Some(MAX_ROW_GROUP_SIZE))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(true)
        .set_bloom_filter_enabled(false);

    // Set the encoding of the decimal column to `PLAIN` to avoid
    // compatibility issues caused by encoding as `Delta_Byte_Array`.
    for field in arrow_schema.fields() {
        if matches!(
            field.data_type(),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
        ) {
            let column = ColumnPath::from(field.name().clone());
            builder = builder.set_column_encoding(column, Encoding::PLAIN);
        }
    }

    let props = builder.build();
    let buf_size = match target_file_size {
        Some(n) if n < MAX_BUFFER_SIZE => n,
        _ => MAX_BUFFER_SIZE,
    };
    Ok(ArrowWriter::try_new(
        Vec::with_capacity(buf_size),
        arrow_schema,
        Some(props),
    )?)
}

impl ParquetEncoder {
    fn try_create(
        info: &CopyIntoLocationInfo,
        schema: TableSchemaRef,
        target_file_size: Option<usize>,
        create_by: String,
    ) -> Result<Self> {
        let arrow_schema = Arc::new(Schema::from(schema.as_ref()));
        let compression = info.stage.file_format_params.compression();
        let compression = match &compression {
            StageFileCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
            StageFileCompression::Snappy => Compression::SNAPPY,
            StageFileCompression::None => Compression::UNCOMPRESSED,
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "unexpected compression {compression}"
                )));
            }
        };
        let writer = create_writer(
            arrow_schema.clone(),
            target_file_size,
            compression,
            create_by.clone(),
        )?;

        Ok(ParquetEncoder {
            arrow_schema,
            compression,
            create_by,
            target_file_size,
            writer,
        })
    }

    fn reinit_writer(&mut self) -> Result<()> {
        self.writer = create_writer(
            self.arrow_schema.clone(),
            self.target_file_size,
            self.compression,
            self.create_by.clone(),
        )?;
        Ok(())
    }
}

impl ColumnarFileEncoder for ParquetEncoder {
    const NAME: &'static str = "ParquetFileWriter";

    fn write(&mut self, block: DataBlock, schema: &TableSchemaRef) -> Result<()> {
        let batch = block.to_record_batch(schema)?;
        self.writer.write(&batch)?;
        Ok(())
    }

    fn bytes_written(&self) -> usize {
        self.writer.bytes_written() + self.writer.in_progress_size()
    }

    fn finish(&mut self) -> Result<Vec<u8>> {
        self.writer.finish().ok();
        let buf = mem::take(self.writer.inner_mut());
        self.reinit_writer()?;
        Ok(buf)
    }
}

impl ParquetFileWriter {
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
        create_by: String,
    ) -> Result<ProcessorPtr> {
        let encoder =
            ParquetEncoder::try_create(&info, schema.clone(), target_file_size, create_by)?;
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
