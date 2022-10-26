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

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_meta_app::schema::CatalogNameIdent;
use common_meta_app::schema::DropCatalogReq;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropCatalogPlan {
    pub if_exists: bool,
    pub tenant: String,
    pub catalog: String,
}

impl DropCatalogPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

impl From<DropCatalogPlan> for DropCatalogReq {
    fn from(p: DropCatalogPlan) -> Self {
        Self {
            if_exists: p.if_exists,
            name_ident: CatalogNameIdent {
                tenant: p.tenant,
                ctl_name: p.catalog,
            },
        }
    }
}

impl From<&DropCatalogPlan> for DropCatalogReq {
    fn from(p: &DropCatalogPlan) -> Self {
        Self {
            if_exists: p.if_exists,
            name_ident: CatalogNameIdent {
                tenant: p.tenant.clone(),
                ctl_name: p.catalog.clone(),
            },
        }
    }
}
