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

use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeHashStatistics;
use crate::pipelines::processors::HashJoinDesc;

pub struct PerformanceContext {
    pub probe_result: ProbedRows,
    pub filter_executor: Option<FilterExecutor>,
    pub probe_hash_statistics: ProbeHashStatistics,
}

impl PerformanceContext {
    pub fn create(
        max_block_size: usize,
        desc: Arc<HashJoinDesc>,
        function_context: FunctionContext,
    ) -> Self {
        let filter_executor = desc.other_predicate.as_ref().map(|predicate| {
            FilterExecutor::new(
                predicate.clone(),
                function_context,
                max_block_size,
                None,
                &BUILTIN_FUNCTIONS,
                false,
            )
        });

        PerformanceContext {
            filter_executor,
            probe_result: ProbedRows::new(
                Vec::with_capacity(max_block_size),
                Vec::with_capacity(max_block_size),
                Vec::with_capacity(max_block_size),
            ),
            probe_hash_statistics: ProbeHashStatistics::new(max_block_size),
        }
    }

    pub fn clear(&mut self) {
        self.probe_result.clear();
        self.probe_hash_statistics.clear(0);
    }
}
