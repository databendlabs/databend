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
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_sql::plans::JoinType;
use databend_common_storage::DataOperator;

use crate::pipelines::processors::transforms::hash_join::spill_common::get_hashes;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

/// Define some states for hash join probe spilling
/// Each processor will have its own state
pub struct ProbeSpillState {
    /// Hash join probe state
    pub probe_state: Arc<HashJoinProbeState>,
    /// Spiller, responsible for specific spill work for hash join probe
    pub spiller: Spiller,
}

impl ProbeSpillState {
    pub fn create(ctx: Arc<QueryContext>, probe_state: Arc<HashJoinProbeState>) -> Result<Self> {
        let tenant = ctx.get_tenant();
        let spill_config = SpillerConfig::create(query_spill_prefix(tenant.tenant_name()));
        let operator = DataOperator::instance().operator();
        let spiller = Spiller::create(ctx, operator, spill_config, SpillerType::HashJoinProbe)?;
        Ok(Self {
            probe_state,
            spiller,
        })
    }

    // Get all hashes for probe input data.
    pub fn get_hashes(
        &self,
        block: &DataBlock,
        join_type: &JoinType,
        hashes: &mut Vec<u64>,
    ) -> Result<()> {
        let func_ctx = self.probe_state.ctx.get_function_context()?;
        let keys = &self.probe_state.hash_join_state.hash_join_desc.probe_keys;
        get_hashes(
            &func_ctx,
            block,
            keys,
            &self.probe_state.hash_method,
            join_type,
            false,
            hashes,
        )
    }
}
