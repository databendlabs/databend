// Copyright 2022 Datafuse Labs.
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

use std::fmt::Debug;
use std::fmt::Formatter;

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::PlanNode;

use crate::api::DataExchange;
use crate::sql::executor::PhysicalPlan;

/// Payload of a `FragmentPlanPacket`, which represents
/// a fragment of a query plan.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum FragmentPayload {
    PlanV1(PlanNode),
    PlanV2(PhysicalPlan),
}

impl Debug for FragmentPayload {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            FragmentPayload::PlanV1(plan) => write!(f, "PlanNode({})", plan.name()),
            FragmentPayload::PlanV2(plan) => write!(f, "PhysicalPlan({:?})", plan),
        }
    }
}

impl FragmentPayload {
    pub fn schema(&self) -> Result<DataSchemaRef> {
        match self {
            FragmentPayload::PlanV1(node) => Ok(node.schema()),
            FragmentPayload::PlanV2(plan) => plan.output_schema(),
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct FragmentPlanPacket {
    pub payload: FragmentPayload,
    pub fragment_id: usize,
    pub data_exchange: Option<DataExchange>,
}

impl FragmentPlanPacket {
    pub fn create(
        fragment_id: usize,
        payload: FragmentPayload,
        data_exchange: Option<DataExchange>,
    ) -> FragmentPlanPacket {
        FragmentPlanPacket {
            payload,
            fragment_id,
            data_exchange,
        }
    }
}

impl Debug for FragmentPlanPacket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentPacket")
            .field("payload", &self.payload)
            .field("fragment_id", &self.fragment_id)
            .field("exchange", &self.data_exchange)
            .finish()
    }
}
