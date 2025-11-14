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

use std::sync::Arc;

use chrono::Utc;
use databend_common_ast::ast::RowAccessPolicyDefinition;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::DropRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::tenant::Tenant;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateRowAccessPolicyPlan {
    pub if_not_exists: bool,
    pub tenant: Tenant,
    pub name: String,
    pub row_access: RowAccessPolicyDefinition,
    pub description: Option<String>,
}

impl CreateRowAccessPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

impl From<CreateRowAccessPolicyPlan> for CreateRowAccessPolicyReq {
    fn from(p: CreateRowAccessPolicyPlan) -> Self {
        CreateRowAccessPolicyReq {
            name: RowAccessPolicyNameIdent::new(p.tenant.clone(), &p.name),
            row_access_policy_meta: RowAccessPolicyMeta {
                // CRITICAL: Normalize parameter names to lowercase at creation time
                // This ensures consistent matching when applying row access policies.
                // SQL identifiers are case-insensitive, so we use lowercase as canonical form.
                // This mirrors the fix for masking policies (see data_mask.rs).
                args: p
                    .row_access
                    .parameters
                    .iter()
                    .map(|arg| {
                        (
                            arg.name.to_lowercase(), // Normalize to lowercase
                            arg.data_type.to_string(),
                        )
                    })
                    .collect(),
                body: p.row_access.definition.to_string(),
                comment: p.description,
                create_on: Utc::now(),
                update_on: None,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DropRowAccessPolicyPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: String,
}

impl DropRowAccessPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

impl From<DropRowAccessPolicyPlan> for DropRowAccessPolicyReq {
    fn from(p: DropRowAccessPolicyPlan) -> Self {
        DropRowAccessPolicyReq {
            name: RowAccessPolicyNameIdent::new(&p.tenant, &p.name),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescRowAccessPolicyPlan {
    pub name: String,
}

impl DescRowAccessPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![
            DataField::new("Name", DataType::String),
            DataField::new("Created On", DataType::String),
            DataField::new("Signature", DataType::String),
            DataField::new("Return Type", DataType::String),
            DataField::new("Body", DataType::String),
            DataField::new("Comment", DataType::String),
        ]))
    }
}
