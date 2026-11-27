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

use std::num::NonZeroUsize;

use databend_common_exception::Result;

use super::ExecutionScope;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::lazy_index_top_k;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

const RULES: &[fn(&PhysicalPlan) -> Option<NonZeroUsize>] = &[bounded_index_top_k];

/// Choose the width once the complete SELECT plan is known, before pruning and
/// pipeline construction consume the statement's settings. A sub-execution
/// shares those settings but must never choose a width for its parent.
pub(super) fn apply_pipeline_width(
    scope: ExecutionScope,
    ctx: &QueryContext,
    plan: &PhysicalPlan,
) -> Result<()> {
    if scope == ExecutionScope::Nested {
        return Ok(());
    }
    let settings = ctx.get_settings();
    if !settings.get_enable_auto_pipeline_width()? {
        return Ok(());
    }
    let overrides = ctx.get_shared_settings();
    if overrides.changes().contains_key("max_threads") || plan.is_distributed_plan() {
        return Ok(());
    }
    if let Some(cap) = RULES.iter().filter_map(|rule| rule(plan)).min()
        && (cap.get() as u64) < settings.get_max_threads()?
    {
        overrides.set_max_threads(cap.get() as u64)?;
        overrides.set_query_level_change(true);
    }
    Ok(())
}

fn bounded_index_top_k(plan: &PhysicalPlan) -> Option<NonZeroUsize> {
    let search = lazy_index_top_k(plan)?;
    // Only the small lazy merge was benchmarked. Larger searches keep their width.
    (search.candidates_per_block <= 100 && (2..=4).contains(&search.blocks))
        .then_some(NonZeroUsize::MIN)
}
