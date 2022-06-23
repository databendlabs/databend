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

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStageStmt {
    pub if_not_exists: bool,
    pub stage_name: String,

    pub location: String,
    pub credential_options: BTreeMap<String, String>,
    pub encryption_options: BTreeMap<String, String>,

    pub file_format_options: BTreeMap<String, String>,
    pub on_error: String,
    pub size_limit: usize,
    pub validation_mode: String,
    pub comments: String,
}
