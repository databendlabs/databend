// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs::File;
use std::task::{Context, Poll};

use arrow::csv;
use csv as csv_crate;
use futures::stream::Stream;

use crate::datablocks::DataBlock;
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;

pub struct CSVStream {
    reader: csv_crate::Reader<File>,
}

impl CSVStream {
    pub fn try_create(schema: DataSchemaRef, r: File) -> FuseQueryResult<Self> {
        let reader = csv::Reader::new(r, schema, false, None, 1024, None, None);
        Ok(CSVStream { reader })
    }
}

impl Stream for CSVStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let batch = self.reader.next();
        match batch {
            Some(Ok(batch)) => {
                let block = DataBlock::try_from_arrow_batch(&batch);
                match block {
                    Ok(block) => Poll::Ready(Some(Ok(block))),
                    _ => Poll::Ready(None),
                }
            }
            _ => Poll::Ready(None),
        }
    }
}
