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

use chrono::DateTime;
use chrono::Utc;
use databend_common_ast::ast::AlterPasswordAction;
use databend_common_ast::ast::PasswordSetOptions;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::PrincipalIdentity;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserOption;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateUserPlan {
    pub create_option: CreateOption,
    pub user: UserIdentity,
    pub auth_info: AuthInfo,
    pub user_option: UserOption,
    pub password_update_on: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterUserPlan {
    pub seq: u64,
    pub user_info: UserInfo,
    pub change_auth: bool,
    pub change_user_option: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropUserPlan {
    pub if_exists: bool,
    pub user: UserIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescUserPlan {
    pub user: UserIdentity,
}

impl DescUserPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("name", DataType::String),
            DataField::new("hostname", DataType::String),
            DataField::new("auth_type", DataType::String),
            DataField::new("default_role", DataType::String),
            DataField::new("default_warehouse", DataType::String),
            DataField::new("roles", DataType::String),
            DataField::new("disabled", DataType::Boolean),
            DataField::new(
                "network_policy",
                DataType::Nullable(Box::new(DataType::String)),
            ),
            DataField::new(
                "password_policy",
                DataType::Nullable(Box::new(DataType::String)),
            ),
            DataField::new(
                "must_change_password",
                DataType::Nullable(Box::new(DataType::Boolean)),
            ),
            DataField::new(
                "workload_group",
                DataType::Nullable(Box::new(DataType::String)),
            ),
        ])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRolePlan {
    pub create_option: CreateOption,
    pub role_name: String,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropRolePlan {
    pub if_exists: bool,
    pub role_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterRolePlan {
    pub if_exists: bool,
    pub role_name: String,
    pub action: AlterRoleAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterRoleAction {
    Comment(Option<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantRolePlan {
    pub principal: PrincipalIdentity,
    pub role: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokeRolePlan {
    pub principal: PrincipalIdentity,
    pub role: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetRolePlan {
    pub is_default: bool,
    pub role_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetSecondaryRolesPlan {
    All,
    None,
    SpecifyRole(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantPrivilegePlan {
    pub principal: PrincipalIdentity,
    pub priv_types: UserPrivilegeSet,
    pub on: GrantObject,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokePrivilegePlan {
    pub principal: PrincipalIdentity,
    pub priv_types: UserPrivilegeSet,
    pub on: Vec<GrantObject>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateNetworkPolicyPlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub name: String,
    pub allowed_ip_list: Vec<String>,
    pub blocked_ip_list: Vec<String>,
    pub comment: String,
}

impl CreateNetworkPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlterNetworkPolicyPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: String,
    pub allowed_ip_list: Option<Vec<String>>,
    pub blocked_ip_list: Option<Vec<String>>,
    pub comment: Option<String>,
}

impl AlterNetworkPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DropNetworkPolicyPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: String,
}

impl DropNetworkPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescNetworkPolicyPlan {
    pub name: String,
}

impl DescNetworkPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("Name", DataType::String),
            DataField::new("Allowed Ip List", DataType::String),
            DataField::new("Blocked Ip List", DataType::String),
            DataField::new("Comment", DataType::String),
        ])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShowNetworkPoliciesPlan {}

impl ShowNetworkPoliciesPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("Name", DataType::String),
            DataField::new("Allowed Ip List", DataType::String),
            DataField::new("Blocked Ip List", DataType::String),
            DataField::new("Comment", DataType::String),
        ])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreatePasswordPolicyPlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub name: String,
    pub set_options: PasswordSetOptions,
}

impl CreatePasswordPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlterPasswordPolicyPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: String,
    pub action: AlterPasswordAction,
}

impl AlterPasswordPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DropPasswordPolicyPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: String,
}

impl DropPasswordPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescPasswordPolicyPlan {
    pub name: String,
}

impl DescPasswordPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("Property", DataType::String),
            DataField::new("Value", DataType::String),
            DataField::new("Default", DataType::Nullable(Box::new(DataType::String))),
            DataField::new("Description", DataType::String),
        ])
    }
}
