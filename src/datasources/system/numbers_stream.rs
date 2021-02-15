// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::{
    task::{Context, Poll},
    usize,
};

use futures::stream::Stream;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataSchemaRef, UInt64Array};
use crate::error::FuseQueryResult;
use std::alloc::Layout;
use std::mem;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::sessions::FuseQueryContextRef;
use arrow::array::ArrayData;
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;

#[derive(Debug, Clone)]
struct BlockRange {
    begin: u64,
    end: u64,
}

pub struct NumbersStream {
    ctx: FuseQueryContextRef,
    schema: DataSchemaRef,
    block_index: usize,
    blocks: Vec<BlockRange>,
}

impl NumbersStream {
    pub fn create(ctx: FuseQueryContextRef, schema: DataSchemaRef) -> Self {
        NumbersStream {
            ctx,
            schema,
            block_index: 0,
            blocks: vec![],
        }
    }

    fn try_get_one_block(&mut self) -> FuseQueryResult<Option<BlockRange>> {
        if (self.block_index as usize) == self.blocks.len() {
            let partitions = self.ctx.try_fetch_partitions(1)?;
            if partitions.is_empty() {
                return Ok(None);
            }

            let block_size = self.ctx.get_max_block_size()?;
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
            self.blocks = blocks;
            self.block_index = 0;
        }

        let current = self.blocks[self.block_index].clone();
        self.block_index += 1;
        Ok(Some(current))
    }
}

impl Stream for NumbersStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let current = self.try_get_one_block()?;
        Poll::Ready(match current {
            None => None,
            Some(current) => {
                let length = (current.end - current.begin) as usize;
                let size = length * mem::size_of::<u64>();

                unsafe {
                    let layout = Layout::from_size_align_unchecked(size, arrow::memory::ALIGNMENT);
                    let p = std::alloc::alloc(layout) as *mut u64;
                    for i in current.begin..current.end {
                        *p.offset((i - current.begin) as isize) = i;
                    }

                    let buffer =
                        Buffer::from_raw_parts(NonNull::new(p as *mut u8).unwrap(), size, size);

                    let arr_data = ArrayData::builder(DataType::UInt64)
                        .len(length)
                        .offset(0)
                        .add_buffer(buffer)
                        .build();

                    let block = DataBlock::create(
                        self.schema.clone(),
                        vec![Arc::new(UInt64Array::from(arr_data))],
                    );
                    Some(Ok(block))
                }
            }
        })
    }
}
