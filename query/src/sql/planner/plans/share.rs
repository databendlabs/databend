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

use common_datavalues::chrono::Utc;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_meta_app::share::CreateShareReq;
use common_meta_app::share::DropShareReq;
use common_meta_app::share::ShareGrantObjectName;
use common_meta_app::share::ShareGrantObjectPrivilege;
use common_meta_app::share::ShareNameIdent;

// Create Share Plan
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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

// Alter Share Accounts Plan
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AlterShareAccountsPlan {
    pub share: String,
    pub if_exists: bool,
    pub accounts: Vec<String>,
    pub add: bool,
}

impl AlterShareAccountsPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
