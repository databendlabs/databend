// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::collections::HashMap;
use std::task::{Context, Poll};
use tokio::stream::Stream;

use crate::datablocks::DataBlock;
use crate::datasources::Partition;
use crate::error::{FuseQueryError, FuseQueryResult};

pub struct MemoryStream {
    index: usize,
    metas: Vec<Partition>,
    partitions: HashMap<String, DataBlock>,
}

impl MemoryStream {
    pub fn create(metas: Vec<Partition>, partitions: HashMap<String, DataBlock>) -> Self {
        MemoryStream {
            index: 0,
            metas,
            partitions,
        }
    }
}

impl Stream for MemoryStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.metas.len() {
            self.index += 1;
            let part = &self.metas[self.index - 1];
            Some(Ok(self
                .partitions
                .get(part.name.as_str())
                .ok_or_else(|| {
                    FuseQueryError::Internal(format!("Can not find the partition: {}", part.name))
                })?
                .clone()))
        } else {
            None
        })
    }
}
