// Copyright 2021 Datafuse Labs.
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

use common_planners::{Part, PartitionsInfo};
use common_planners::Partitions;
use crate::table_functions::numbers_part::NumbersPartInfo;

struct MemoryPartInfo {
    total: u64,
    part_start: u64,
    part_end: u64,
}

pub fn generate_block_parts(start: u64, workers: u64, total: u64) -> PartitionsInfo {
    let part_size = total / workers;
    let part_remain = total % workers;

    let mut partitions = Vec::with_capacity(workers as usize);
    if part_size == 0 {
        partitions.push(NumbersPartInfo::create(total, start, total));
    } else {
        for part in 0..workers {
            let mut part_begin = part * part_size;
            if part == 0 && start > 0 {
                part_begin = start;
            }
            let mut part_end = (part + 1) * part_size;
            if part == (workers - 1) && part_remain > 0 {
                part_end += part_remain;
            }

            partitions.push(NumbersPartInfo::create(part_begin, part_end, total));
        }
    }

    partitions
}
