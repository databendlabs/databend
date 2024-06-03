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
use databend_common_storages_fuse::operations::need_reserve_block_info;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn merge_into_get_optimization_flag(&self, join: &HashJoin) -> (bool, bool) {
        // for merge into target table as build side.
        match &*join.build {
            PhysicalPlan::TableScan(scan) => match scan.table_index {
                None | Some(databend_common_sql::DUMMY_TABLE_INDEX) => (false, false),
                Some(table_index) => match need_reserve_block_info(self.ctx.clone(), table_index) {
                    // due to issue https://github.com/datafuselabs/databend/issues/15643,
                    // target build optimization of merge-into is disabled

                    //(true, is_distributed) => (true, is_distributed),
                    (true, is_distributed) => (false, is_distributed),
                    _ => (false, false),
                },
            },
            _ => (false, false),
        }
    }
}
