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

use std::sync::Arc;

use chrono::Utc;
use common_datavalues::prelude::*;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::SchemaDataType;
use common_meta_app::share::CreateShareReq;
use common_meta_app::share::DropShareReq;
use common_meta_app::share::ShareGrantObjectName;
use common_meta_app::share::ShareGrantObjectPrivilege;
use common_meta_app::share::ShareNameIdent;

// Create Share Plan
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSharePlan {
    pub if_not_exists: bool,
    pub tenant: String,
    pub share: String,
    pub comment: Option<String>,
}

impl From<CreateSharePlan> for CreateShareReq {
    fn from(p: CreateSharePlan) -> Self {
        CreateShareReq {
            if_not_exists: p.if_not_exists,
            share_name: ShareNameIdent {
                tenant: p.tenant,
                share_name: p.share,
            },
            comment: p.comment,
            create_on: Utc::now(),
        }
    }
}

impl CreateSharePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// Drop Share Plan
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSharePlan {
    pub if_exists: bool,
    pub tenant: String,
    pub share: String,
}

impl From<DropSharePlan> for DropShareReq {
    fn from(p: DropSharePlan) -> Self {
        DropShareReq {
            if_exists: p.if_exists,
            share_name: ShareNameIdent {
                tenant: p.tenant,
                share_name: p.share,
            },
        }
    }
}

impl DropSharePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// Grant Share Object Plan
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantShareObjectPlan {
    pub share: String,
    pub object: ShareGrantObjectName,
    pub privilege: ShareGrantObjectPrivilege,
}

impl GrantShareObjectPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// Revoke Share Object Plan
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokeShareObjectPlan {
    pub share: String,
    pub object: ShareGrantObjectName,
    pub privilege: ShareGrantObjectPrivilege,
}

impl RevokeShareObjectPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// Alter Share Tenants Plan
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterShareTenantsPlan {
    pub share: String,
    pub if_exists: bool,
    pub accounts: Vec<String>,
    pub is_add: bool,
}

impl AlterShareTenantsPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// desc share
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescSharePlan {
    pub share: String,
}

impl DescSharePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![
            DataField::new("Kind", DataType::String),
            DataField::new("Name", DataType::String),
            DataField::new("Shared_on", DataType::String),
        ]))
    }
}

// show share
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowSharesPlan {}

impl ShowSharesPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![
            DataField::new("Created_on", DataType::String),
            DataField::new("Kind", DataType::String),
            DataField::new("Name", DataType::String),
            DataField::new("Database_name", DataType::String),
            DataField::new("From", DataType::String),
            DataField::new("To", DataType::String),
            DataField::new("Comment", DataType::String),
        ]))
    }
}

// Show object grant privileges.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowObjectGrantPrivilegesPlan {
    pub object: ShareGrantObjectName,
}

impl ShowObjectGrantPrivilegesPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![
            DataField::new("Granted_on", DataType::String),
            DataField::new("Privilege", DataType::String),
            DataField::new("Share_name", DataType::String),
        ]))
    }
}

// Show grant tenants of share.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowGrantTenantsOfSharePlan {
    pub share_name: String,
}

impl ShowGrantTenantsOfSharePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![
            DataField::new("Granted_on", DataType::String),
            DataField::new("Account", DataType::String),
        ]))
    }
}
