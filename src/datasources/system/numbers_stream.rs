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
    pub fn create(block_size: u64, schema: DataSchemaRef, partitions: Partitions) -> Self {
        let mut blocks = Vec::with_capacity(partitions.len());

        for part in partitions {
            let names: Vec<_> = part.name.split('-').collect();
            let begin: u64 = names[1].parse().unwrap();
            let end: u64 = names[2].parse().unwrap();

            let diff = end - begin;
            let block_nums = diff / block_size;
            let block_remain = diff % block_size;

            if block_nums == 0 {
                blocks.push(BlockRange { begin, end });
            } else {
                for r in 0..block_nums {
                    let range_begin = begin + block_size * r;
                    let mut range_end = range_begin + block_size;
                    if r == (block_nums - 1) && block_remain > 0 {
                        range_end += block_remain;
                    }
                    blocks.push(BlockRange {
                        begin: range_begin,
                        end: range_end,
                    });
                }
            }
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

            let data: Vec<u64> = (current.begin..current.end).collect();
            let block =
                DataBlock::create(self.schema.clone(), vec![Arc::new(UInt64Array::from(data))]);
            Some(Ok(block))
        } else {
            None
        })
    }
}
