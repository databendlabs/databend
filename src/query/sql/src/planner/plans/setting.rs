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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VarValue {
    pub is_global: bool,
    pub variable: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettingPlan {
    pub vars: Vec<VarValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnSettingPlan {
    pub session_level: bool,
    pub vars: Vec<String>,
}
