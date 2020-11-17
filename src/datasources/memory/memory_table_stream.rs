// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::stream::Stream;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::datablocks::DataBlock;
use crate::datasources::{ITable, Partition};
use crate::error::Result;

pub struct MemoryTableStream {
    index: usize,
    partitions: Vec<Partition>,
    table: Arc<dyn ITable>,
}

impl MemoryTableStream {
    pub fn new(partitions: Vec<Partition>, table: Arc<dyn ITable>) -> Self {
        MemoryTableStream {
            index: 0,
            partitions,
            table,
        }
    }
}

impl Stream for MemoryTableStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.partitions.len() {
            self.index += 1;
            let part = &self.partitions[self.index - 1];
            Some(self.table.read_partition(part))
        } else {
            None
        })
    }
}
