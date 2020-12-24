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
    pub fn create(max_block_size: u64, schema: DataSchemaRef, partitions: Partitions) -> Self {
        let mut blocks = vec![];
        let block_size = max_block_size;

        for part in partitions {
            let names: Vec<_> = part.name.split('-').collect();
            let begin: u64 = names[1].parse().unwrap();
            let end: u64 = names[2].parse().unwrap();
            let count = end - begin + 1;

            let block_nums = count / block_size;
            let remain = count % block_size;

            if block_nums > 0 {
                for i in 0..block_nums {
                    let block_begin = begin + block_size * i;
                    let mut block_end = begin + block_size * (i + 1) - 1;
                    if (i == block_nums - 1) && remain > 0 {
                        block_end = block_begin + remain;
                    }
                    blocks.push(BlockRange {
                        begin: block_begin,
                        end: block_end,
                    });
                }
            } else {
                blocks.push(BlockRange { begin, end });
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

            let data: Vec<u64> = (current.begin..=current.end).collect();
            let block =
                DataBlock::create(self.schema.clone(), vec![Arc::new(UInt64Array::from(data))]);
            Some(Ok(block))
        } else {
            None
        })
    }
}
