// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io;
use std::io::BufRead;
use std::io::BufReader;

use common_planners::Partition;
use common_planners::Partitions;

pub fn generate_parts(workers: u64, total: u64) -> Partitions {
    let part_size = total / workers;
    let part_remain = total % workers;

    let mut partitions = Vec::with_capacity(workers as usize);
    if part_size == 0 {
        partitions.push(Partition {
            name: format!("{}-{}-{}", total, 0, total,),
            version: 0,
        })
    } else {
        for part in 0..workers {
            let part_begin = part * part_size;
            let mut part_end = (part + 1) * part_size;
            if part == (workers - 1) && part_remain > 0 {
                part_end += part_remain;
            }
            partitions.push(Partition {
                name: format!("{}-{}-{}", total, part_begin, part_end,),
                version: 0,
            })
        }
    }
    partitions
}

/// Counts lines in the source `handle`.
/// count_lines(std::fs::File.open("foo.txt")
pub fn count_lines<R: io::Read>(handle: R) -> Result<usize, io::Error> {
    let sep = b'\n';
    let mut reader = BufReader::new(handle);
    let mut count = 0;
    let mut line: Vec<u8> = Vec::new();
    while match reader.read_until(sep, &mut line) {
        Ok(n) if n > 0 => true,
        Err(e) => return Err(e),
        _ => false,
    } {
        if *line.last().unwrap() == sep {
            count += 1;
        };
    }
    Ok(count)
}
