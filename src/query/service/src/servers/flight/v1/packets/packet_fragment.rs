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

use std::fmt::Debug;
use std::fmt::Formatter;

use crate::physical_plans::PhysicalPlan;
use crate::servers::flight::v1::exchange::DataExchange;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

impl Debug for QueryFragment {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("QueryFragment")
            .field("physical_plan", &self.physical_plan)
            .field("fragment_id", &self.fragment_id)
            .field("exchange", &self.data_exchange)
            .finish()
    }
}
