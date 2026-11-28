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

use databend_common_exception::Result;

use crate::physical_plans::PhysicalPlan;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

/// Only top-level statement preparation may narrow the query's settings.
pub(super) fn apply_pipeline_width(ctx: &QueryContext, root: &PhysicalPlan) -> Result<()> {
    let settings = ctx.get_settings();
    if !settings.get_enable_auto_pipeline_width()? {
        return Ok(());
    }
    let overrides = ctx.get_shared_settings();
    if overrides.changes().contains_key("max_threads") {
        return Ok(());
    }
    if let Some(cap) = root.max_threads_hint()
        && cap.get() < settings.get_max_threads()?
        && !root.is_distributed_plan()
    {
        overrides.set_max_threads(cap.get())?;
        overrides.set_query_level_change(true);
    }
    Ok(())
}
