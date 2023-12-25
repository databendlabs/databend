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

use std::sync::Arc;

use databend_common_arrow::parquet::metadata::FileMetaData;
use databend_common_arrow::parquet::metadata::RowGroupMetaData;
use log::debug;

use crate::HiveBlockFilter;
use crate::HivePartInfo;

#[derive(Clone)]
pub struct HiveBlocks {
    pub file_meta: Arc<FileMetaData>,
    pub part: HivePartInfo,
    pub valid_rowgroups: Vec<usize>,
    pub current_index: usize,
    pub hive_block_filter: Arc<HiveBlockFilter>,
}

impl HiveBlocks {
    pub fn create(
        file_meta: Arc<FileMetaData>,
        part: HivePartInfo,
        hive_block_filter: Arc<HiveBlockFilter>,
    ) -> Self {
        Self {
            file_meta,
            part,
            valid_rowgroups: vec![],
            current_index: 0,
            hive_block_filter,
        }
    }

    // there are some conditions to filter invalid row_groups:
    // 1. the rowgroup doesn't belong to the partition
    // 2. filtered by predict pushdown
    pub fn prune(&mut self) -> bool {
        let mut pruned_rg_cnt = 0;
        for (idx, row_group) in self.file_meta.row_groups.iter().enumerate() {
            let start = row_group.columns()[0].byte_range().0;
            let mid = start + row_group.compressed_size() as u64 / 2;
            if !self.part.range.contains(&mid) {
                continue;
            }
            if self
                .hive_block_filter
                .filter(row_group, self.part.get_partition_map())
            {
                pruned_rg_cnt += 1;
            } else {
                self.valid_rowgroups.push(idx);
            }
        }
        debug!(
            "hive parquet predict pushdown have pruned {} rowgroups",
            pruned_rg_cnt
        );
        self.has_blocks()
    }

    pub fn get_part_info(&self) -> HivePartInfo {
        self.part.clone()
    }

    pub fn get_current_row_group_meta_data(&self) -> &RowGroupMetaData {
        &self.file_meta.row_groups[self.get_current_rowgroup_index()]
    }

    pub fn advance(&mut self) {
        self.current_index += 1;
    }

    pub fn has_blocks(&self) -> bool {
        self.current_index < self.valid_rowgroups.len()
    }

    fn get_current_rowgroup_index(&self) -> usize {
        self.valid_rowgroups[self.current_index]
    }
}
