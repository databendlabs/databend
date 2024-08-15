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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct TenantQuota {
    // The max databases can be created in the tenant.
    pub max_databases: u32,

    // The max tables per database can be created in the tenant.
    pub max_tables_per_database: u32,

    // The max stages can be created in the tenant.
    pub max_stages: u32,

    // The max files per stage can be created in the tenant.
    pub max_files_per_stage: u32,

    // The max number of users can be created in the tenant.
    pub max_users: u32,
}
