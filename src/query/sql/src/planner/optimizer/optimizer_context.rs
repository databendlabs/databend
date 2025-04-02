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

use databend_common_catalog::table_context::TableContext;
use educe::Educe;

use crate::planner::QueryExecutor;
use crate::MetadataRef;

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct OptimizerContext {
    #[educe(Debug(ignore))]
    pub(crate) table_ctx: Arc<dyn TableContext>,
    pub(crate) metadata: MetadataRef,

    // Optimizer configurations
    pub(crate) enable_distributed_optimization: bool,
    pub(crate) enable_join_reorder: bool,
    pub(crate) enable_dphyp: bool,
    pub(crate) max_push_down_limit: usize,
    pub(crate) planning_agg_index: bool,
    #[educe(Debug(ignore))]
    pub(crate) sample_executor: Option<Arc<dyn QueryExecutor>>,
}

impl OptimizerContext {
    pub fn new(table_ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        Self {
            table_ctx,
            metadata,

            enable_distributed_optimization: false,
            enable_join_reorder: true,
            enable_dphyp: true,
            max_push_down_limit: 10000,
            sample_executor: None,
            planning_agg_index: false,
        }
    }

    pub fn with_enable_distributed_optimization(mut self, enable: bool) -> Self {
        self.enable_distributed_optimization = enable;
        self
    }

    pub fn with_enable_join_reorder(mut self, enable: bool) -> Self {
        self.enable_join_reorder = enable;
        self
    }

    pub fn with_enable_dphyp(mut self, enable: bool) -> Self {
        self.enable_dphyp = enable;
        self
    }

    pub fn with_sample_executor(mut self, sample_executor: Option<Arc<dyn QueryExecutor>>) -> Self {
        self.sample_executor = sample_executor;
        self
    }

    pub fn with_planning_agg_index(mut self) -> Self {
        self.planning_agg_index = true;
        self
    }

    pub fn with_max_push_down_limit(mut self, max_push_down_limit: usize) -> Self {
        self.max_push_down_limit = max_push_down_limit;
        self
    }
}
