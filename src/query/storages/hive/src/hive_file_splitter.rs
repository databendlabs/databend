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

use std::ops::Range;
use std::sync::Arc;

use common_planners::PartInfo;

use crate::HiveFileInfo;
use crate::HivePartInfo;

#[derive(Clone, Debug)]
pub struct HiveFileSplitter {
    min_split_size: u64,
}

impl HiveFileSplitter {
    pub fn create(min_split_size: u64) -> Self {
        Self { min_split_size }
    }

    pub fn split_length(&self, length: u64) -> Vec<Range<u64>> {
        let length = length;
        let mut num = length / self.min_split_size;
        let left = length % self.min_split_size;
        if num == 0 || left > self.min_split_size / 3 {
            num += 1;
        }

        let mut res = vec![];
        for i in 0..num {
            let start = i * self.min_split_size;
            let end = match i == num - 1 {
                true => length + 1,
                false => (i + 1) * self.min_split_size,
            };
            res.push(start..end);
        }
        res
    }

    fn split_single_file(&self, hive_file_info: HiveFileInfo) -> Vec<Arc<Box<dyn PartInfo>>> {
        let splits = self.split_length(hive_file_info.length);
        splits
            .into_iter()
            .map(|r| {
                HivePartInfo::create(
                    hive_file_info.filename.clone(),
                    hive_file_info.partition.clone(),
                    r,
                )
            })
            .collect()
    }

    pub fn get_splits(&self, files: Vec<HiveFileInfo>) -> Vec<Arc<Box<dyn PartInfo>>> {
        files
            .into_iter()
            .flat_map(|hive_file| self.split_single_file(hive_file))
            .collect::<Vec<Arc<Box<dyn PartInfo>>>>()
    }
}
