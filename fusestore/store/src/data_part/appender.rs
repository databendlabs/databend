// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::bail;
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

use crate::fs::IFileSystem;

#[allow(dead_code)] // temporarily allowed
pub(crate) struct Appender<FS> {
    fs: FS // owned type? to be discussed later.
}

#[allow(dead_code)] // temporarily allowed
impl<FS> Appender<FS>
where FS: IFileSystem
{
    pub fn new(fs: FS) -> Self {
        Appender { fs }
    }

    /// Assumes
    /// - upstream caller has properly batched data
    /// - first element of the incoming stream is a properly serialized schema
    pub async fn append_data(
        &self,
        path: String,
        mut stream: std::pin::Pin<Box<dyn futures::Stream<Item = FlightData>>>
    ) -> Result<Vec<String>> {
        if let Some(flight_data) = stream.next().await {
            let data_schema = DataSchema::try_from(&flight_data)?;

            let schema_ref = Arc::new(data_schema);

            let mut appended_files: Vec<String> = Vec::new();
            while let Some(flight_data) = stream.next().await {
                //            let mut block_stream = stream.map(move |flight_data| {
                let batch = flight_data_to_arrow_batch(&flight_data, schema_ref.clone(), &[])?;
                let block = DataBlock::try_from(batch)?;
                let buffer = write_in_memory(block)?;
                let part_uuid = Uuid::new_v4().to_simple().to_string() + ".parquet";
                let part_file_name = format!("{}/{}", path, part_uuid);
                self.fs.add(part_file_name, &buffer).await?;
                appended_files.push(part_uuid);
            }

            Ok(appended_files)
        } else {
            bail!("empty stream")
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
