// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use super::plan_do_readsource::PlanDoReadSource;
use crate::optimizers::Optimizers;
use crate::sessions::DatabendQueryContextRef;

pub fn apply_plan_rewrite(
    context: DatabendQueryContextRef,
    optimizer: Optimizers,
    plan: &PlanNode,
) -> Result<PlanNode> {
    let mut optimizer = optimizer;
    eprintln!("DDD apply_plan_rewrite before optimize: {:?}", plan);
    let plan = optimizer.optimize(plan)?;
    eprintln!("DDD apply_plan_rewrite after optimize: {:?}", plan);
    let plan = PlanDoReadSource::create(context).rewrite_plan_node(&plan)?;
    eprintln!(
        "DDD apply_plan_rewrite after plan do read source rewrite: {:?}",
        plan
    );
    Ok(plan)
}
