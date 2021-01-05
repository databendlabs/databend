// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::task::{Context, Poll};

use futures::stream::Stream;

use crate::datablocks::DataBlock;
use crate::datasources::Partitions;
use crate::datavalues::{DataSchemaRef, UInt64Array};
use crate::error::FuseQueryResult;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct BlockRange {
    begin: u64,
    end: u64,
}

pub struct NumbersStream {
    block_index: usize,
    schema: DataSchemaRef,
    blocks: Vec<BlockRange>,
}

impl NumbersStream {
    pub fn create(schema: DataSchemaRef, partitions: Partitions) -> Self {
        let mut blocks = vec![];

        for part in partitions {
            let names: Vec<_> = part.name.split('-').collect();
            let begin: u64 = names[1].parse().unwrap();
            let end: u64 = names[2].parse().unwrap();
            blocks.push(BlockRange { begin, end });
        }

        NumbersStream {
            block_index: 0,
            schema,
            blocks,
        }
    }
}

impl Stream for NumbersStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if (self.block_index as usize) < self.blocks.len() {
            let current = self.blocks[self.block_index].clone();
            self.block_index += 1;

            let data: Vec<u64> = (current.begin..=current.end).collect();
            let block =
                DataBlock::create(self.schema.clone(), vec![Arc::new(UInt64Array::from(data))]);
            Some(Ok(block))
        } else {
            None
        })
    }
}
