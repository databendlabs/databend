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
use databend_common_ast::ast::DataMaskPolicy;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::DropDatamaskReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateDatamaskPolicyPlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub name: String,
    pub policy: DataMaskPolicy,
}

impl CreateDatamaskPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

impl From<CreateDatamaskPolicyPlan> for CreateDatamaskReq {
    fn from(p: CreateDatamaskPolicyPlan) -> Self {
        CreateDatamaskReq {
            create_option: p.create_option,
            name: DataMaskNameIdent::new(p.tenant.clone(), &p.name),
            data_mask_meta: DatamaskMeta {
                args: p
                    .policy
                    .args
                    .iter()
                    .map(|arg| (arg.arg_name.to_string(), arg.arg_type.to_string()))
                    .collect(),
                return_type: p.policy.return_type.to_string(),
                body: p.policy.body.to_string(),
                comment: p.policy.comment,
                create_on: Utc::now(),
                update_on: None,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DropDatamaskPolicyPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: String,
}

impl DropDatamaskPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

impl From<DropDatamaskPolicyPlan> for DropDatamaskReq {
    fn from(p: DropDatamaskPolicyPlan) -> Self {
        DropDatamaskReq {
            if_exists: p.if_exists,
            name: DataMaskNameIdent::new(&p.tenant, &p.name),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescDatamaskPolicyPlan {
    pub name: String,
}

impl DescDatamaskPolicyPlan {
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
