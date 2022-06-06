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

use std::fmt;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct UserIdentity {
    pub username: String,
    pub hostname: String,
}

impl UserIdentity {
    pub fn new(name: &str, host: &str) -> Self {
        Self {
            username: name.to_string(),
            hostname: host.to_string(),
        }
    }

    pub fn is_localhost(&self) -> bool {
        &self.hostname.to_lowercase() == "localhost" || &self.hostname == "127.0.0.1"
    }

    pub fn is_root(&self) -> bool {
        self.username.eq("root") || self.username.eq("default")
    }
}

impl fmt::Display for UserIdentity {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        write!(f, "'{}'@'{}'", self.username, self.hostname)
    }
}
