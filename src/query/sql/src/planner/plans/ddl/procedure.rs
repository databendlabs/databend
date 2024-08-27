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

use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::principal::CreateProcedureReq;
use databend_common_meta_app::principal::DropProcedureReq;
use databend_common_meta_app::principal::ProcedureMeta;
use databend_common_meta_app::principal::ProcedureNameIdent;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

#[derive(Clone, Debug, PartialEq)]
pub struct ExecuteImmediatePlan {
    pub script: String,
}

impl ExecuteImmediatePlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![DataField::new("Result", DataType::String)])
    }
}

#[derive(Debug, Clone)]
pub struct CreateProcedurePlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub name: String,
    pub meta: ProcedureMeta,
}

impl From<CreateProcedurePlan> for CreateProcedureReq {
    fn from(p: CreateProcedurePlan) -> Self {
        CreateProcedureReq {
            create_option: p.create_option,
            name_ident: ProcedureNameIdent::new(&p.tenant, &p.name),
            meta: p.meta,
        }
    }
}

impl From<&CreateProcedurePlan> for CreateProcedureReq {
    fn from(p: &CreateProcedurePlan) -> Self {
        CreateProcedureReq {
            create_option: p.create_option,
            name_ident: ProcedureNameIdent::new(&p.tenant, &p.name),
            meta: p.meta.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DropProcedurePlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: ProcedureNameIdent,
}

impl From<DropProcedurePlan> for DropProcedureReq {
    fn from(p: DropProcedurePlan) -> Self {
        DropProcedureReq {
            if_exists: p.if_exists,
            name_ident: p.name,
        }
    }
}

impl From<&DropProcedurePlan> for DropProcedureReq {
    fn from(p: &DropProcedurePlan) -> Self {
        DropProcedureReq {
            if_exists: p.if_exists,
            name_ident: p.name.clone(),
        }
    }
}
