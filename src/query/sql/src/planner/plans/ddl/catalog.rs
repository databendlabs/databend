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

use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::CreateCatalogReq;
use databend_common_meta_app::schema::DropCatalogReq;
use databend_common_meta_app::tenant::Tenant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateCatalogPlan {
    pub if_not_exists: bool,
    pub tenant: Tenant,
    pub catalog: String,
    pub meta: CatalogMeta,
}

impl From<CreateCatalogPlan> for CreateCatalogReq {
    fn from(p: CreateCatalogPlan) -> Self {
        CreateCatalogReq {
            if_not_exists: p.if_not_exists,
            name_ident: CatalogNameIdent::new(p.tenant, p.catalog),
            meta: p.meta,
        }
    }
}

impl From<&CreateCatalogPlan> for CreateCatalogReq {
    fn from(p: &CreateCatalogPlan) -> Self {
        CreateCatalogReq {
            if_not_exists: p.if_not_exists,
            name_ident: CatalogNameIdent::new(p.tenant.clone(), p.catalog.clone()),
            meta: p.meta.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropCatalogPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub catalog: String,
}

impl From<DropCatalogPlan> for DropCatalogReq {
    fn from(value: DropCatalogPlan) -> DropCatalogReq {
        DropCatalogReq {
            if_exists: value.if_exists,
            name_ident: CatalogNameIdent::new(value.tenant, value.catalog),
        }
    }
}

impl From<&DropCatalogPlan> for DropCatalogReq {
    fn from(value: &DropCatalogPlan) -> DropCatalogReq {
        DropCatalogReq {
            if_exists: value.if_exists,
            name_ident: CatalogNameIdent::new(value.tenant.clone(), value.catalog.clone()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowCreateCatalogPlan {
    pub catalog: String,
    pub schema: DataSchemaRef,
}

impl ShowCreateCatalogPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
