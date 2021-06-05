// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::FlightData;
use common_arrow::parquet::arrow::ArrowWriter;
use common_arrow::parquet::file::writer::InMemoryWriteableCursor;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
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
    pub async fn append_data(
        &self,
        path: String,
        mut stream: InputData,
    ) -> Result<common_flights::AppendResult> {
        if let Some(flight_data) = stream.next().await {
            let data_schema = DataSchema::try_from(&flight_data)?;
            let schema_ref = Arc::new(data_schema);
            let mut result = common_flights::AppendResult::default();
            while let Some(flight_data) = stream.next().await {
                let batch = flight_data_to_arrow_batch(&flight_data, schema_ref.clone(), &[])?;
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
    let cursor = InMemoryWriteableCursor::default();
    {
        let cursor = cursor.clone();
        let batch = RecordBatch::try_from(block)?;
        let mut writer = ArrowWriter::try_new(cursor, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
    }
    cursor
        .into_inner()
        .context("failed to convert cursor into vector of u8")
}
