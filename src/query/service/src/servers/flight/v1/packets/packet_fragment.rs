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

use std::any::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;

use serde::de::Error;
use serde::Deserializer;
use serde::Serializer;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::PhysicalPlanMeta;
use crate::servers::flight::v1::exchange::DataExchange;

#[derive(Clone)]
pub struct QueryFragment {
    pub fragment_id: usize,
    pub data_exchange: Option<DataExchange>,
    pub physical_plan: PhysicalPlan,
}

impl QueryFragment {
    pub fn create(
        fragment_id: usize,
        data_exchange: Option<DataExchange>,
        physical_plan: PhysicalPlan,
    ) -> QueryFragment {
        QueryFragment {
            physical_plan,
            fragment_id,
            data_exchange,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SerializedPhysicalPlanRef(u32);

impl IPhysicalPlan for SerializedPhysicalPlanRef {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_meta(&self) -> &PhysicalPlanMeta {
        unimplemented!()
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        unimplemented!()
    }

    fn derive(&self, _: Vec<PhysicalPlan>) -> PhysicalPlan {
        unimplemented!()
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct SerializeQueryFragment {
    pub fragment_id: usize,
    pub data_exchange: Option<DataExchange>,
    pub flatten_plan: VecDeque<PhysicalPlan>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct SerializeQueryFragmentJson {
    pub fragment_id: usize,
    pub data_exchange: Option<DataExchange>,
    pub flatten_plan: VecDeque<serde_json::Value>,
}

impl serde::Serialize for QueryFragment {
    #[recursive::recursive]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut flatten_queue = vec![self.physical_plan.clone()];
        let mut flattened = VecDeque::new();

        while let Some(mut plan) = flatten_queue.pop() {
            for child in plan.children_mut() {
                let id = child.get_id();
                let mut new_child = PhysicalPlan::new(SerializedPhysicalPlanRef(id));
                std::mem::swap(&mut new_child, child);
                flatten_queue.push(new_child);
            }

            flattened.push_front(plan);
        }

        let serialize_fragment = SerializeQueryFragment {
            fragment_id: self.fragment_id,
            data_exchange: self.data_exchange.clone(),
            flatten_plan: flattened,
        };

        serialize_fragment.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for QueryFragment {
    #[recursive::recursive]
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fragment = SerializeQueryFragmentJson::deserialize(deserializer)?;

        // Deserialize PhysicalPlan node-by-node using Value to avoid streaming issues.
        let flatten_plan: VecDeque<PhysicalPlan> = fragment
            .flatten_plan
            .into_iter()
            .map(|plan| {
                serde_json::from_value(plan)
                    .map_err(|e| D::Error::custom(format!("deserialize physical plan: {e}")))
            })
            .collect::<Result<_, _>>()?;
        let mut fragment = SerializeQueryFragment {
            fragment_id: fragment.fragment_id,
            data_exchange: fragment.data_exchange,
            flatten_plan,
        };

        let mut flatten_storage = HashMap::new();

        while let Some(mut plan) = fragment.flatten_plan.pop_front() {
            for child in plan.children_mut() {
                if let Some(ref_plan) = SerializedPhysicalPlanRef::from_physical_plan(child) {
                    let Some(mut flatten_plan) = flatten_storage.remove(&ref_plan.0) else {
                        return Err(D::Error::custom(""));
                    };

                    std::mem::swap(child, &mut flatten_plan);
                }
            }

            flatten_storage.insert(plan.get_id(), plan);
        }

        assert_eq!(flatten_storage.len(), 1);
        Ok(QueryFragment {
            fragment_id: fragment.fragment_id,
            data_exchange: fragment.data_exchange,
            physical_plan: flatten_storage.into_values().next().unwrap(),
        })
    }
}

impl Debug for QueryFragment {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("QueryFragment")
            .field("physical_plan", &self.physical_plan)
            .field("fragment_id", &self.fragment_id)
            .field("exchange", &self.data_exchange)
            .finish()
    }
}
