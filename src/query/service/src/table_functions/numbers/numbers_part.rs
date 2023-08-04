// Copyright 2021 Datafuse Labs
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

use std::any::Any;
use std::sync::Arc;

use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_exception::ErrorCode;
use common_exception::Result;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NumbersPartInfo {
    pub total: u64,
    pub part_start: u64,
    pub part_end: u64,
}

#[typetag::serde(name = "numbers")]
impl PartInfo for NumbersPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<NumbersPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        0
    }
}

impl NumbersPartInfo {
    pub fn create(start: u64, end: u64, total: u64) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(NumbersPartInfo {
            total,
            part_start: start,
            part_end: end,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&NumbersPartInfo> {
        info.as_any()
            .downcast_ref::<NumbersPartInfo>()
            .ok_or(ErrorCode::Internal(
                "Cannot downcast from PartInfo to NumbersPartInfo.",
            ))
    }
}

pub fn generate_numbers_parts(start: u64, workers: u64, total: u64) -> Partitions {
    let part_size = total / workers;
    let part_remain = total % workers;

    let mut partitions = Vec::with_capacity(workers as usize);
    if part_size == 0 {
        partitions.push(NumbersPartInfo::create(start, total, total));
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

    Partitions::create_nolazy(PartitionsShuffleKind::Seq, partitions)
}
