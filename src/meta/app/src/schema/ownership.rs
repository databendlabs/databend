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

use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct Ownership {
    pub owner_role_name: String,
    pub updated_on: DateTime<Utc>,
}

impl Ownership {
    pub fn new(owner_role_name: String) -> Self {
        Ownership {
            owner_role_name,
            updated_on: Utc::now(),
        }
    }
}

impl Default for Ownership {
    fn default() -> Self {
        Ownership {
            owner_role_name: "".to_string(),
            updated_on: Utc::now(),
        }
    }
}

impl Display for Ownership {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "OwnerRoleName: {}, UpdatedOn: {:?}",
            self.owner_role_name, self.updated_on
        )
    }
}
