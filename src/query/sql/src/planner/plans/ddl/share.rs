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

use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSharePlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub name: String,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSharePlan {
    pub tenant: Tenant,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterSharePlan {
    pub tenant: Tenant,
    pub if_exists: bool,
    pub name: String,
    pub action: AlterSharePlanAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterSharePlanAction {
    AddAccounts {
        accounts: Vec<String>,
    },
    RemoveAccounts {
        accounts: Vec<String>,
    },
    Set {
        accounts: Option<Vec<String>>,
        comment: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantSharePlan {
    pub tenant: Tenant,
    pub share: String,
    pub privilege: ShareGrantObjectPrivilege,
    pub object: ShareGrantObject,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokeSharePlan {
    pub tenant: Tenant,
    pub share: String,
    pub privilege: ShareGrantObjectPrivilege,
    pub object: ShareGrantObject,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShareGrantObjectPrivilege {
    Usage,
    Select,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShareGrantObject {
    Database {
        database: String,
    },
    Table {
        database: Option<String>,
        table: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowSharesPlan {
    pub tenant: Tenant,
    pub like: Option<String>,
    pub limit: Option<u64>,
}

impl ShowSharesPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("created_on", DataType::String),
            DataField::new("kind", DataType::String),
            DataField::new("owner_account", DataType::String),
            DataField::new("name", DataType::String),
            DataField::new("database_name", DataType::String),
            DataField::new("to", DataType::String),
            DataField::new("owner", DataType::String),
            DataField::new("comment", DataType::String),
            DataField::new("listing_global_name", DataType::String),
        ])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescSharePlan {
    pub tenant: Tenant,
    pub provider_tenant: Option<String>,
    pub share: String,
}

impl DescSharePlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("kind", DataType::String),
            DataField::new("name", DataType::String),
            DataField::new("shared_on", DataType::String),
        ])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDatabaseFromSharePlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub provider_tenant: String,
    pub share: String,
}
