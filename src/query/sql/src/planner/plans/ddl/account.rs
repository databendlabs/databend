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

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::PrincipalIdentity;
use common_meta_types::UserIdentity;
use common_meta_types::UserOption;
use common_meta_types::UserPrivilegeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateUserPlan {
    pub user: UserIdentity,
    pub auth_info: AuthInfo,
    pub user_option: UserOption,
    pub if_not_exists: bool,
}

impl CreateUserPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterUserPlan {
    pub user: UserIdentity,
    // None means no change to make
    pub auth_info: Option<AuthInfo>,
    pub user_option: Option<UserOption>,
}

impl AlterUserPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropUserPlan {
    pub if_exists: bool,
    pub user: UserIdentity,
}

impl DropUserPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRolePlan {
    pub if_not_exists: bool,
    pub role_name: String,
}

impl CreateRolePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropRolePlan {
    pub if_exists: bool,
    pub role_name: String,
}

impl DropRolePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantRolePlan {
    pub principal: PrincipalIdentity,
    pub role: String,
}

impl GrantRolePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowGrantsPlan {
    pub principal: Option<PrincipalIdentity>,
}

impl ShowGrantsPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![DataField::new("Grants", Vu8::to_data_type())])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokeRolePlan {
    pub principal: PrincipalIdentity,
    pub role: String,
}

impl RevokeRolePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetRolePlan {
    pub is_default: bool,
    pub role_name: String,
}

impl SetRolePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowRolesPlan {}

impl ShowRolesPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("inherited_roles", u64::to_data_type()),
            DataField::new("is_current", bool::to_data_type()),
            DataField::new("is_default", bool::to_data_type()),
        ])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantPrivilegePlan {
    pub principal: PrincipalIdentity,
    pub priv_types: UserPrivilegeSet,
    pub on: GrantObject,
}

impl GrantPrivilegePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokePrivilegePlan {
    pub principal: PrincipalIdentity,
    pub priv_types: UserPrivilegeSet,
    pub on: GrantObject,
}

impl RevokePrivilegePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
