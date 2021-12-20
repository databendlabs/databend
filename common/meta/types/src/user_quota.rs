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

use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
#[serde(default)]
pub struct UserQuota {
    // The max cpu can be used (0 is no limited).
    pub max_cpu: u64,

    // The max memory(bytes) can be used(0 is no limited).
    pub max_memory_in_bytes: u64,

    // The max storage(bytes) can be used(0 is no limited).
    pub max_storage_in_bytes: u64,
}

impl UserQuota {
    pub fn no_limit() -> Self {
        UserQuota {
            max_cpu: 0,
            max_memory_in_bytes: 0,
            max_storage_in_bytes: 0,
        }
    }
}
