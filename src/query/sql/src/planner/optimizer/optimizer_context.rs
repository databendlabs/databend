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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use educe::Educe;
use parking_lot::RwLock;

use crate::planner::QueryExecutor;
use crate::MetadataRef;

#[derive(Educe)]
#[educe(Debug)]
pub struct OptimizerContext {
    #[educe(Debug(ignore))]
    table_ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,

    // Optimizer configurations
    enable_distributed_optimization: RwLock<bool>,
    enable_join_reorder: RwLock<bool>,
    enable_dphyp: RwLock<bool>,
    max_push_down_limit: RwLock<usize>,
    planning_agg_index: RwLock<bool>,
    #[educe(Debug(ignore))]
    sample_executor: RwLock<Option<Arc<dyn QueryExecutor>>>,

    // Optimizer state flags
    #[educe(Debug(ignore))]
    flags: RwLock<HashMap<String, bool>>,
}

impl OptimizerContext {
    pub fn new(table_ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Arc<Self> {
        Arc::new(Self {
            table_ctx,
            metadata,

            enable_distributed_optimization: RwLock::new(false),
            enable_join_reorder: RwLock::new(true),
            enable_dphyp: RwLock::new(true),
            max_push_down_limit: RwLock::new(10000),
            sample_executor: RwLock::new(None),
            planning_agg_index: RwLock::new(false),
            flags: RwLock::new(HashMap::new()),
        })
    }

    pub fn get_table_ctx(self: &Arc<Self>) -> Arc<dyn TableContext> {
        self.table_ctx.clone()
    }

    pub fn get_metadata(self: &Arc<Self>) -> MetadataRef {
        self.metadata.clone()
    }

    pub fn set_enable_distributed_optimization(self: &Arc<Self>, enable: bool) -> &Arc<Self> {
        *self.enable_distributed_optimization.write() = enable;
        self
    }

    pub fn get_enable_distributed_optimization(self: &Arc<Self>) -> bool {
        *self.enable_distributed_optimization.read()
    }

    pub fn set_enable_join_reorder(self: &Arc<Self>, enable: bool) -> &Arc<Self> {
        *self.enable_join_reorder.write() = enable;
        self
    }

    pub fn get_enable_join_reorder(self: &Arc<Self>) -> bool {
        *self.enable_join_reorder.read()
    }

    pub fn set_enable_dphyp(self: &Arc<Self>, enable: bool) -> &Arc<Self> {
        *self.enable_dphyp.write() = enable;
        self
    }

    pub fn get_enable_dphyp(self: &Arc<Self>) -> bool {
        *self.enable_dphyp.read()
    }

    pub fn set_sample_executor(
        self: &Arc<Self>,
        sample_executor: Option<Arc<dyn QueryExecutor>>,
    ) -> Arc<Self> {
        *self.sample_executor.write() = sample_executor;
        self.clone()
    }

    pub fn get_sample_executor(self: &Arc<Self>) -> Option<Arc<dyn QueryExecutor>> {
        self.sample_executor.read().clone()
    }

    pub fn set_planning_agg_index(self: &Arc<Self>, enable: bool) -> &Arc<Self> {
        *self.planning_agg_index.write() = enable;
        self
    }

    pub fn get_planning_agg_index(self: &Arc<Self>) -> bool {
        *self.planning_agg_index.read()
    }

    pub fn set_max_push_down_limit(self: &Arc<Self>, max_push_down_limit: usize) -> &Arc<Self> {
        *self.max_push_down_limit.write() = max_push_down_limit;
        self
    }

    pub fn get_max_push_down_limit(self: &Arc<Self>) -> usize {
        *self.max_push_down_limit.read()
    }

    pub fn set_flag(self: &Arc<Self>, name: &str, value: bool) -> &Arc<Self> {
        let mut flags = self.flags.write();
        flags.insert(name.to_string(), value);
        self
    }

    pub fn get_flag(self: &Arc<Self>, name: &str) -> bool {
        let flags = self.flags.read();
        *flags.get(name).unwrap_or(&false)
    }
}
