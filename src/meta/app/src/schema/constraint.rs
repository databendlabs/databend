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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum Constraint {
    Check(String),
}

impl Constraint {
    pub fn type_name(&self) -> &'static str {
        match self {
            Constraint::Check(_) => "check",
        }
    }

    pub fn expr(&self) -> String {
        match self {
            Constraint::Check(expr) => expr.clone(),
        }
    }
}
