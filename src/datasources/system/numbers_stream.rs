// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::debug;
use std::time::Instant;

use std::task::{Context, Poll};
use tokio::stream::Stream;

use crate::datablocks::DataBlock;
use crate::datasources::Partitions;
use crate::datavalues::Int64Array;
use crate::datavalues::{DataField, DataSchema, DataType};
use crate::error::FuseQueryResult;
use std::sync::Arc;

pub struct NumbersStream {
    index: usize,
    partitions: Partitions,
}

impl NumbersStream {
    pub fn create(partitions: Partitions) -> Self {
        NumbersStream {
            index: 0,
            partitions,
        }
    }
}

impl Stream for NumbersStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.partitions.len() {
            let start = Instant::now();

            let part = self.partitions[self.index].clone();
            let names: Vec<_> = part.name.split('-').collect();
            let begin: i64 = names[1].parse()?;
            let end: i64 = names[2].parse()?;

            self.index += 1;
            let data: Vec<i64> = (begin..=end).collect();
            let block = DataBlock::create(
                Arc::new(DataSchema::new(vec![DataField::new(
                    "number",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(data))],
            );
            let duration = start.elapsed();
            debug!("number stream part:{}, cost:{:?}", part.name, duration);
            Some(Ok(block))
        } else {
            None
        })
    }
}
