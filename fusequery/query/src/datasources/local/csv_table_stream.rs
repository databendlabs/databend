// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::fs::File;
use std::task::Poll;

use anyhow::Context;
use common_exception::{Result, ErrorCodes};
use common_arrow::arrow::csv;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use futures::Stream;

use crate::sessions::FuseQueryContextRef;

pub struct CsvTableStream {
    ctx: FuseQueryContextRef,
    file: String,
    schema: DataSchemaRef
}

impl CsvTableStream {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        schema: DataSchemaRef,
        file: String
    ) -> Result<Self> {
        Ok(CsvTableStream { ctx, file, schema })
    }

    pub fn try_get_one_block(&self) -> Result<Option<DataBlock>> {
        let partitions = self.ctx.try_get_partitions(1).map_err(ErrorCodes::from_anyhow)?;
        if partitions.is_empty() {
            return Ok(None);
        }

        let part = partitions[0].clone();
        let names: Vec<_> = part.name.split('-').collect();
        let begin: usize = names[1].parse().map_err(ErrorCodes::from_parse)?;
        let end: usize = names[2].parse().map_err(ErrorCodes::from_parse)?;
        let bounds = Some((begin, end));
        let block_size = end - begin;

        let file = File::open(self.file.clone())
            .with_context(|| format!("Failed to read csv file:{}", self.file.clone())).map_err(ErrorCodes::from_anyhow)?;
        let mut reader: csv::Reader<File> = csv::Reader::new(
            file,
            self.schema.clone(),
            false,
            None,
            block_size,
            bounds,
            None,
        );

        reader.next().map(|record| {
            record.map_err(ErrorCodes::from_arrow).and_then(|record| {
                record.try_into()
            })
        }).map(|data_block| data_block.map(|v| Some(v))).unwrap_or_else(|| Ok(None))
    }
}

impl Stream for CsvTableStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>
    ) -> Poll<Option<Self::Item>> {
        let block = self.try_get_one_block()?;
        Poll::Ready(block.map(Ok))
    }
}
