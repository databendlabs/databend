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

use databend_common_ast::ast::QuotaValueStmt;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CreateWorkloadGroupPlan {
    pub name: String,
    pub if_not_exists: bool,
    pub quotas: BTreeMap<String, QuotaValueStmt>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct DropWorkloadGroupPlan {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RenameWorkloadGroupPlan {
    pub name: String,
    pub new_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SetWorkloadGroupQuotasPlan {
    pub name: String,
    pub quotas: BTreeMap<String, QuotaValueStmt>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct UnsetWorkloadGroupQuotasPlan {
    pub name: String,
    pub quotas: Vec<String>,
}
