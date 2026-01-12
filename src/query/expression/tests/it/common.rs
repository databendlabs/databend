// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;

use databend_common_expression::BlockIndex;
use databend_common_expression::DataBlock;

type MergeSlice = (usize, usize, usize);

pub fn run_filter(file: &mut impl Write, predicate: Vec<bool>, block: &DataBlock) {
    let result = block.clone().filter_with_bitmap(&predicate.clone().into());

    match result {
        Ok(result_block) => {
            writeln!(file, "Filter:         {predicate:?}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

pub fn run_concat(file: &mut impl Write, blocks: &[DataBlock]) {
    let total_block_size: usize = blocks.iter().map(|b| b.memory_size()).sum();
    let result = DataBlock::concat(blocks);

    match result {
        Ok(result_block) => {
            let result_block_size = result_block.memory_size();
            // bits could be compressed into one byte in nullable column
            assert!(
                total_block_size >= result_block_size
                    && total_block_size as f64 <= result_block_size as f64 * 1.5f64
            );
            for (i, c) in blocks.iter().enumerate() {
                writeln!(file, "Concat-Column {i}:").unwrap();
                writeln!(file, "{c:?}").unwrap();
            }
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

pub fn run_take_block(file: &mut impl Write, indices: &[BlockIndex], blocks: &[DataBlock]) {
    let result = DataBlock::take_blocks(blocks, indices);
    writeln!(file, "Take Block indices:         {indices:?}").unwrap();
    for (i, block) in blocks.iter().enumerate() {
        writeln!(file, "Block{i}:\n{block}").unwrap();
    }
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

pub fn run_take_block_by_slices_with_limit(
    file: &mut impl Write,
    slices: &[MergeSlice],
    blocks: &[DataBlock],
    limit: Option<usize>,
) {
    let result = DataBlock::take_by_slices_limit_from_blocks(blocks, slices, limit);
    writeln!(
        file,
        "Take Block by slices (limit: {limit:?}):       {slices:?}"
    )
    .unwrap();
    for (i, block) in blocks.iter().enumerate() {
        writeln!(file, "Block{i}:\n{block}").unwrap();
    }
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

pub fn run_scatter(file: &mut impl Write, block: &DataBlock, indices: &[u32], scatter_size: usize) {
    let result = DataBlock::scatter(block, indices, scatter_size);

    match result {
        Ok(result_block) => {
            writeln!(file, "Scatter:         {indices:?}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();

            for (i, c) in result_block.iter().enumerate() {
                writeln!(file, "Result-{i}:\n{c}").unwrap();
            }
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

pub fn run_take(file: &mut impl Write, indices: &[u32], block: &DataBlock) {
    let result = DataBlock::take(block, indices);

    match result {
        Ok(result_block) => {
            writeln!(file, "Take:         {indices:?}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}
