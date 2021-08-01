// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::convert::TryFrom;
use std::io::Cursor;
use std::sync::Arc;

use anyhow::Result;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::FlightData;
// use common_arrow::parquet::arrow::ArrowWriter;
// use common_arrow::parquet::file::writer::InMemoryWriteableCursor;
use common_datablocks::DataBlock;
use common_flights::storage_api_impl::AppendResult;
use futures::StreamExt;
use uuid::Uuid;

use crate::fs::FileSystem;

pub(crate) struct Appender {
    fs: Arc<dyn FileSystem>,
}

pub type InputData = std::pin::Pin<Box<dyn futures::Stream<Item = FlightData> + Send>>;

impl Appender {
    pub fn new(fs: Arc<dyn FileSystem>) -> Self {
        Appender { fs }
    }

    /// Assumes
    /// - upstream caller has properly batched data
    /// - first element of the incoming stream is a properly serialized schema
    pub async fn append_data(&self, path: String, mut stream: InputData) -> Result<AppendResult> {
        if let Some(flight_data) = stream.next().await {
            let arrow_schema = ArrowSchema::try_from(&flight_data)?;
            let arrow_schema_ref = Arc::new(arrow_schema);

            let mut result = AppendResult::default();
            while let Some(flight_data) = stream.next().await {
                let batch =
                    flight_data_to_arrow_batch(&flight_data, arrow_schema_ref.clone(), false, &[])?;
                let block = DataBlock::try_from(batch)?;
                let (rows, cols, wire_bytes) =
                    (block.num_rows(), block.num_columns(), block.memory_size());
                let part_uuid = Uuid::new_v4().to_simple().to_string() + ".parquet";
                let location = format!("{}/{}", path, part_uuid);
                let buffer = write_in_memory(block)?;

                result.append_part(&location, rows, cols, wire_bytes, buffer.len());

                self.fs.add(&location, &buffer).await?;
            }
            Ok(result)
        } else {
            anyhow::bail!("Schema of input data must be provided")
        }
    }
}

pub(crate) fn write_in_memory(block: DataBlock) -> Result<Vec<u8>> {
    let arrow_schema = block.schema().to_arrow();
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionCodec::Uncompressed,
        version: Version::V2,
    };
    let encoding = Encoding::Plain;
    let batch = RecordBatch::try_from(block)?;

    let iter = vec![Ok(batch)];

    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &arrow_schema, options, vec![
        Encoding::Plain,
    ])?;

    // Create a new empty file
    let writer = Vec::with_capacity(block.memory_size());
    let mut cursor = Cursor::new(writer);
    // Write the file. Note that, at present, any error results in a corrupted file.
    let parquet_schema = row_groups.parquet_schema().clone();
    write_file(
        &mut cursor,
        row_groups,
        &arrow_schema,
        parquet_schema,
        options,
        None,
    )?;

    Ok(cursor.into_inner())
}
