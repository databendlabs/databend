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

use std::time::Duration;

#[derive(Clone)]
pub struct QueryProfile {
    /// Query ID of the query profile
    pub query_id: String,

    /// Flattened plan node profiles
    pub plan_node_profs: Vec<PlanNodeProfile>,
}

impl QueryProfile {
    pub fn new(query_id: String, plan_node_profs: Vec<PlanNodeProfile>) -> Self {
        QueryProfile {
            query_id,
            plan_node_profs,
        }
    }
}

#[derive(Clone)]
pub struct PlanNodeProfile {
    /// ID of the `PhysicalPlan`
    pub id: u32,

    /// Name of the `PhysicalPlan`, e.g. `HashJoin`
    pub plan_node_name: String,

    // TODO(leiysky): making this a typed enum
    /// Description of the `PhysicalPlan`
    pub description: String,

    /// The time spent to process in nanoseconds
    pub cpu_time: Duration,
}

impl PlanNodeProfile {
    pub fn new(id: u32, plan_node_name: String, description: String, cpu_time: Duration) -> Self {
        PlanNodeProfile {
            id,
            plan_node_name,
            description,
            cpu_time,
        }
    }
}
