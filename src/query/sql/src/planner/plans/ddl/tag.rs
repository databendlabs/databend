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

use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

/// Plan for `CREATE TAG`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTagPlan {
    pub tenant: Tenant,
    pub create_option: CreateOption,
    pub name: String,
    pub allowed_values: Option<Vec<String>>,
    pub comment: Option<String>,
}

/// Plan for `DROP TAG`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTagPlan {
    pub tenant: Tenant,
    pub if_exists: bool,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagSetPlanItem {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseTagSetTarget {
    pub if_exists: bool,
    pub catalog: String,
    pub database: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableTagSetTarget {
    pub if_exists: bool,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageTagSetTarget {
    pub if_exists: bool,
    pub stage_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionTagSetTarget {
    pub if_exists: bool,
    pub connection_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewTagSetTarget {
    pub if_exists: bool,
    pub catalog: String,
    pub database: String,
    pub view: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UDFTagSetTarget {
    pub if_exists: bool,
    pub udf_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcedureTagSetTarget {
    pub if_exists: bool,
    pub name: String,
    pub args: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TagSetObject {
    Database(DatabaseTagSetTarget),
    Table(TableTagSetTarget),
    Stage(StageTagSetTarget),
    Connection(ConnectionTagSetTarget),
    View(ViewTagSetTarget),
    UDF(UDFTagSetTarget),
    Procedure(ProcedureTagSetTarget),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetObjectTagsPlan {
    pub tenant: Tenant,
    pub object: TagSetObject,
    pub tags: Vec<TagSetPlanItem>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsetObjectTagsPlan {
    pub tenant: Tenant,
    pub object: TagSetObject,
    pub tags: Vec<String>,
}
