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

use std::fmt;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct RoleIdentity {
    pub name: String,
    pub host: String,
}

impl RoleIdentity {
    pub fn new(name: String, host: String) -> Self {
        Self { name, host }
    }

    pub fn parse(identity: &str) -> Self {
        let chunks: Vec<_> = identity.split('@').collect();
        let name = chunks[0].trim_matches('\'').to_string();
        if chunks.len() <= 1 {
            return Self {
                name,
                host: "%".to_string(),
            };
        }
        let host = chunks[1].trim_matches('\'').to_string();
        Self { name, host }
    }
}

impl fmt::Display for RoleIdentity {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        write!(f, "'{}'@'{}'", self.name, self.host)
    }
}
