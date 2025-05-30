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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;

use super::Exchange;
use super::FragmentKind;
use crate::executor::PhysicalPlan;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BroadcastSource {
    pub plan_id: u32,
    pub broadcast_id: u32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BroadcastSink {
    pub plan_id: u32,
    pub broadcast_id: u32,
    pub input: Box<PhysicalPlan>,
}

pub fn build_broadcast_plan(broadcast_id: u32) -> Result<PhysicalPlan> {
    let broadcast_source = Box::new(PhysicalPlan::BroadcastSource(BroadcastSource {
        plan_id: 0,
        broadcast_id,
    }));
    let exchange = Box::new(PhysicalPlan::Exchange(Exchange {
        plan_id: 0,
        input: broadcast_source,
        kind: FragmentKind::Expansive,
        keys: vec![],
        allow_adjust_parallelism: true,
        ignore_exchange: false,
    }));
    let broadcast_sink = PhysicalPlan::BroadcastSink(BroadcastSink {
        plan_id: 0,
        broadcast_id,
        input: exchange,
    });
    Ok(broadcast_sink)
}

pub fn build_broadcast_plans(ctx: &dyn TableContext) -> Result<Vec<PhysicalPlan>> {
    let mut plans = vec![];
    let next_broadcast_id = ctx.get_next_broadcast_id();
    ctx.reset_broadcast_id();
    for broadcast_id in 0..next_broadcast_id {
        plans.push(build_broadcast_plan(broadcast_id)?);
    }
    Ok(plans)
}
