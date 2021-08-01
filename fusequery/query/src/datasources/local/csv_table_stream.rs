// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs::File;
use std::sync::Arc;
use std::task::Poll;

use common_arrow::arrow::io::csv::read;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::Stream;

use crate::sessions::FuseQueryContextRef;

pub struct CsvTableStream {
    ctx: FuseQueryContextRef,
    file: String,
    schema: DataSchemaRef,
}

impl CsvTableStream {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        schema: DataSchemaRef,
        file: String,
    ) -> Result<Self> {
        Ok(CsvTableStream { ctx, file, schema })
    }

    pub fn try_get_one_block(&self) -> Result<Option<DataBlock>> {
        let partitions = self.ctx.try_get_partitions(1)?;
        if partitions.is_empty() {
            return Ok(None);
        }

        let part = partitions[0].clone();
        let names: Vec<_> = part.name.split('-').collect();
        let begin: usize = names[1].parse()?;
        let end: usize = names[2].parse()?;
        let block_size = end - begin;

        let arrow_schema = Arc::new(self.schema.to_arrow());
        let mut reader = read::ReaderBuilder::new()
            .from_path(&self.file)
            .map_err(|e| ErrorCode::CannotReadFile(e.to_string()))?;

        let mut rows = vec![read::ByteRecord::default(); block_size];
        let rows_read = read::read_rows(&mut reader, begin, &mut rows)?;
        let rows = &rows[..rows_read];

        let record = read::deserialize_batch(
            rows,
            arrow_schema.fields(),
            None,
            0,
            read::deserialize_column,
        )?;

        let block = DataBlock::try_from(record)?;
        Ok(Some(block))
    }
}

impl Stream for CsvTableStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let block = self.try_get_one_block()?;
        Poll::Ready(block.map(Ok))
    }
}
