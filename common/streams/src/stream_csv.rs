// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::fs::File;
use std::task::Poll;

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use common_arrow::arrow::csv;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use csv as csv_crate;
use futures::Stream;

pub struct CsvStream {
    reader: csv_crate::Reader<File>,
}

impl CsvStream {
    pub fn try_create(schema: DataSchemaRef, file: String, has_header: bool) -> Result<Self> {
        let f =
            File::open(file.clone()).with_context(|| format!("Failed to read file:{}", file))?;
        let reader = csv::Reader::new(f, schema, has_header, None, 1024, None, None);
        Ok(CsvStream { reader })
    }
}

impl Stream for CsvStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.reader.next() {
            Some(result) => match result {
                Ok(batch) => {
                    let block = batch.try_into();
                    match block {
                        Ok(block) => Poll::Ready(Some(Ok(block))),
                        Err(e) => Poll::Ready(Some(Err(e))),
                    }
                }
                Err(e) => Poll::Ready(Some(Err(anyhow!("{:?}", e)))),
            },
            None => Poll::Ready(None),
        }
    }
}
