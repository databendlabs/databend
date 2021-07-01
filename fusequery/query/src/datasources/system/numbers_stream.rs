// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::mem::ManuallyDrop;
use std::mem::{self};
use std::ptr::NonNull;
use std::task::Context;
use std::task::Poll;
use std::usize;

use common_arrow::arrow::array::ArrayData;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::UInt64Array;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_streams::ProgressStream;
use futures::stream::Stream;

use crate::sessions::FuseQueryContextRef;

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
    pub fn try_create(ctx: FuseQueryContextRef, schema: DataSchemaRef) -> Result<ProgressStream> {
        let stream = Box::pin(NumbersStream {
            ctx: ctx.clone(),
            schema,
            block_index: 0,
            blocks: vec![],
        });
        ProgressStream::try_create(stream, ctx.progress_callback()?)
    }

    fn try_get_one_block(&mut self) -> Result<Option<DataBlock>> {
        if (self.block_index as usize) == self.blocks.len() {
            let partitions = self.ctx.try_get_partitions(1)?;
            if partitions.is_empty() {
                return Ok(None);
            }

            let block_size = self.ctx.get_settings().get_max_block_size()?;
            let mut blocks = Vec::with_capacity(partitions.len());

            for part in partitions {
                let names: Vec<_> = part.name.split('-').collect();
                let begin: u64 = names[1].parse()?;
                let end: u64 = names[2].parse()?;

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

        Ok(if current.begin == current.end {
            None
        } else {
            let v = (current.begin..current.end).collect::<Vec<u64>>();
            let mut me = ManuallyDrop::new(v);
            let byte_size = mem::size_of::<u64>();

            unsafe {
                let buffer = Buffer::from_raw_parts(
                    NonNull::new(me.as_mut_ptr() as *mut u8).unwrap(),
                    me.len() * byte_size,
                    me.capacity() * byte_size,
                );

                let arr_data = ArrayData::builder(ArrowDataType::UInt64)
                    .len(me.len())
                    .offset(0)
                    .add_buffer(buffer)
                    .build();

                let array = Arc::new(UInt64Array::from(arr_data)) as ArrayRef;
                let block =
                    DataBlock::create_by_array(self.schema.clone(), vec![array.into_series()]);
                Some(block)
            }

            // let mut av = AlignedVec::with_capacity_aligned((current.end - current.begin) as usize);
            // for val in current.begin..current.end {
            //     av.push(val);
            // }
            // let series = DFUInt64Array::new_from_aligned_vec(av).into_series();
            // let block = DataBlock::create_by_array(self.schema.clone(), vec![series]);
            // Some(block)
        })
    }
}

impl Stream for NumbersStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let block = self.try_get_one_block()?;

        Poll::Ready(block.map(Ok))
    }
}
