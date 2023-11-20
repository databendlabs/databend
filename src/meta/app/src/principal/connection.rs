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

use std::collections::BTreeMap;

use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct UserDefinedConnection {
    pub name: String,
    pub storage_type: String,
    pub storage_params: BTreeMap<String, String>,
}

impl UserDefinedConnection {
    pub fn new(name: &str, storage_type: String, storage_params: BTreeMap<String, String>) -> Self {
        Self {
            name: name.to_string(),
            storage_type: storage_type.to_lowercase(),
            storage_params: storage_params
                .into_iter()
                .map(|(k, v)| (k.to_lowercase(), v))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    pub fn storage_params_display(&self) -> String {
        self.storage_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .join(" ")
    }
}
