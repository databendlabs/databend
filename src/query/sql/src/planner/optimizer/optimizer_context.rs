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
use databend_common_exception::Result;
use databend_common_settings::Settings;
use educe::Educe;
use parking_lot::RwLock;

use crate::optimizer::optimizers::rule::RuleID;
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

    // Enable optimizer tracing
    #[educe(Debug(ignore))]
    enable_trace: RwLock<bool>,
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
            enable_trace: RwLock::new(false),
        })
    }

    pub fn with_settings(self: Arc<Self>, settings: &Settings) -> Result<Arc<Self>> {
        self.set_enable_join_reorder(unsafe { !settings.get_disable_join_reorder()? });
        *self.enable_dphyp.write() = settings.get_enable_dphyp()?;
        *self.max_push_down_limit.write() = settings.get_max_push_down_limit()?;
        *self.enable_trace.write() = settings.get_enable_optimizer_trace()?;

        Ok(self)
    }

    pub fn get_table_ctx(&self) -> Arc<dyn TableContext> {
        self.table_ctx.clone()
    }

    pub fn get_metadata(&self) -> MetadataRef {
        self.metadata.clone()
    }

    pub fn set_enable_distributed_optimization(self: &Arc<Self>, enable: bool) -> &Arc<Self> {
        *self.enable_distributed_optimization.write() = enable;
        self
    }

    pub fn get_enable_distributed_optimization(&self) -> bool {
        *self.enable_distributed_optimization.read()
    }

    fn set_enable_join_reorder(self: &Arc<Self>, enable: bool) -> &Arc<Self> {
        *self.enable_join_reorder.write() = enable;
        self
    }

    pub fn get_enable_join_reorder(&self) -> bool {
        *self.enable_join_reorder.read()
    }

    pub fn get_enable_dphyp(&self) -> bool {
        *self.enable_dphyp.read()
    }

    pub fn set_sample_executor(
        self: &Arc<Self>,
        sample_executor: Option<Arc<dyn QueryExecutor>>,
    ) -> &Arc<Self> {
        *self.sample_executor.write() = sample_executor;
        self
    }

    pub fn get_sample_executor(&self) -> Option<Arc<dyn QueryExecutor>> {
        self.sample_executor.read().clone()
    }

    pub fn set_planning_agg_index(self: &Arc<Self>, enable: bool) -> &Arc<Self> {
        *self.planning_agg_index.write() = enable;
        self
    }

    pub fn get_planning_agg_index(&self) -> bool {
        *self.planning_agg_index.read()
    }

    pub fn get_max_push_down_limit(&self) -> usize {
        *self.max_push_down_limit.read()
    }

    pub fn set_flag(self: &Arc<Self>, name: &str, value: bool) -> &Arc<Self> {
        let mut flags = self.flags.write();
        flags.insert(name.to_string(), value);
        self
    }

    pub fn get_flag(&self, name: &str) -> bool {
        let flags = self.flags.read();
        *flags.get(name).unwrap_or(&false)
    }

    pub fn get_enable_trace(&self) -> bool {
        *self.enable_trace.read()
    }

    /// Check if an optimizer or rule is disabled based on optimizer_skip_list setting
    pub fn is_optimizer_disabled(&self, name: &str) -> bool {
        let settings = self.get_table_ctx().get_settings();

        if !settings.get_grouping_sets_to_union().unwrap_or_default()
            && (name == RuleID::GroupingSetsToUnion.to_string()
                || name == RuleID::HierarchicalGroupingSetsToUnion.to_string())
        {
            return true;
        }

        match settings.get_optimizer_skip_list() {
            Ok(skip_list) if !skip_list.is_empty() => {
                let name_lower = name.to_lowercase();
                let is_disabled = skip_list
                    .split(',')
                    .map(str::trim)
                    .any(|item| item.to_lowercase() == name_lower);

                if is_disabled {
                    log::warn!(
                        "Skipping optimizer component: {} (found in optimizer_skip_list: {})",
                        name,
                        skip_list
                    );
                }
                is_disabled
            }
            _ => false,
        }
    }
}
