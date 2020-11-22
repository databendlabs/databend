// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fs::File;
use std::task::{Context, Poll};
use tokio::stream::Stream;

use crate::datablocks::DataBlock;
use crate::datasources::Partition;
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;

pub struct CsvStream {
    index: usize,
    partitions: Vec<Partition>,
    batch_size: usize,
    schema: DataSchemaRef,
    reader: arrow::csv::Reader<File>,
}

impl CsvStream {
    pub fn try_create(
        partitions: Vec<Partition>,
        batch_size: usize,
        schema: DataSchemaRef,
    ) -> FuseQueryResult<Self> {
        let filename = partitions[0].name.clone();
        let file = File::open(filename)?;
        let reader =
            arrow::csv::Reader::new(file, schema.clone(), true, Some(b','), batch_size, None);

        Ok(CsvStream {
            index: 1,
            partitions,
            batch_size,
            schema,
            reader,
        })
    }
}

impl Stream for CsvStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.reader.next() {
                None => {
                    if self.index < self.partitions.len() {
                        let filename = self.partitions[self.index].name.clone();
                        let file = File::open(filename.clone())?;
                        self.reader = arrow::csv::Reader::new(
                            file,
                            self.schema.clone(),
                            true,
                            Some(b','),
                            self.batch_size,
                            None,
                        );
                        self.index += 1;
                    } else {
                        return Poll::Ready(None);
                    }
                }
                Some(v) => {
                    let arrow_batch: arrow::record_batch::RecordBatch = v.unwrap();
                    return Poll::Ready(Some(DataBlock::create_from_arrow_batch(&arrow_batch)));
                }
            }
        }
    }
}
