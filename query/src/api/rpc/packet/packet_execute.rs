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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExecutePacket {
    pub query_id: String,
    pub executor: String,
    // We send nodes info for each node. This is a bad choice
    pub executors_info: HashMap<String, Arc<NodeInfo>>,
}

impl ExecutePacket {
    pub fn create(
        query_id: String,
        executor: String,
        executors_info: HashMap<String, Arc<NodeInfo>>,
    ) -> ExecutePacket {
        ExecutePacket {
            query_id,
            executor,
            executors_info,
        }
    }
}
