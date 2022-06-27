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

use std::collections::HashMap;
use std::sync::Arc;

use common_meta_types::NodeInfo;

use crate::api::rpc::packet::packet_fragment::FragmentPacket;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExecutorPacket {
    pub query_id: String,
    pub executor: String,
    pub request_executor: String,
    pub fragments: Vec<FragmentPacket>,
    // We send nodes info for each node. This is a bad choice
    pub executors_info: HashMap<String, Arc<NodeInfo>>,
    pub source_2_fragments: HashMap<String, Vec<usize>>,
}

impl ExecutorPacket {
    pub fn create(
        query_id: String,
        executor: String,
        fragments: Vec<FragmentPacket>,
        executors_info: HashMap<String, Arc<NodeInfo>>,
        source_2_fragments: HashMap<String, Vec<usize>>,
        request_executor: String,
    ) -> ExecutorPacket {
        ExecutorPacket {
            query_id,
            executor,
            fragments,
            executors_info,
            request_executor,
            source_2_fragments,
        }
    }
}
