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

use common_planners::PlanNode;

use crate::api::DataExchange;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct FragmentPacket {
    pub node: PlanNode,
    pub fragment_id: usize,
    pub data_exchange: Option<DataExchange>,
}

impl FragmentPacket {
    pub fn create(
        fragment_id: usize,
        node: PlanNode,
        data_exchange: Option<DataExchange>,
    ) -> FragmentPacket {
        FragmentPacket {
            node,
            fragment_id,
            data_exchange,
        }
    }
}

impl Debug for FragmentPacket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentPacket")
            .field("node", &self.node.name())
            .field("fragment_id", &self.fragment_id)
            .field("exchange", &self.data_exchange)
            .finish()
    }
}
