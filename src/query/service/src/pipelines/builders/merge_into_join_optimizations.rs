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

use databend_common_sql::executor::physical_plans::HashJoin;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::IndexType;
use databend_common_sql::DUMMY_TABLE_INDEX;
use databend_common_storages_fuse::operations::need_reserve_block_info;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn merge_into_get_optimization_flag(&self, join: &HashJoin) -> (IndexType, bool) {
        // for merge into target table as build side.
        let (merge_into_build_table_index, merge_into_is_distributed) =
            if let PhysicalPlan::TableScan(scan) = &*join.build {
                let (need_block_info, is_distributed) =
                    need_reserve_block_info(self.ctx.clone(), scan.table_index);
                if need_block_info {
                    (scan.table_index, is_distributed)
                } else {
                    (DUMMY_TABLE_INDEX, false)
                }
            } else {
                (DUMMY_TABLE_INDEX, false)
            };
        (merge_into_build_table_index, merge_into_is_distributed)
    }
}
