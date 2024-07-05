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

use databend_common_catalog::catalog::CatalogManager;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CatalogIdIdent;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::plans::CreateCatalogPlan;
use databend_common_storages_fuse::TableContext;
use log::debug;

use super::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct CreateCatalogInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateCatalogPlan,
}

impl CreateCatalogInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateCatalogPlan) -> Result<Self> {
        Ok(CreateCatalogInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateCatalogInterpreter {
    fn name(&self) -> &str {
        "CreateCatalogInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "create_catalog_execute");

        let catalog_manager = CatalogManager::instance();

        // Build and check if catalog is valid.
        let ctl_info = CatalogInfo {
            id: CatalogIdIdent::new(Tenant::new_literal("dummy"), 0).into(),
            name_ident: CatalogNameIdent::new(self.plan.tenant.clone(), &self.plan.catalog).into(),
            meta: CatalogMeta {
                catalog_option: self.plan.meta.catalog_option.clone(),
                created_on: chrono::Utc::now(),
            },
        };
        let ctl = catalog_manager
            .build_catalog(Arc::new(ctl_info), self.ctx.txn_mgr())
            .map_err(|err| err.add_message("Error creating catalog."))?;

        // list databases to check if the catalog is valid.
        let _ = ctl.list_databases(&self.plan.tenant).await.map_err(|err| {
            err.add_message("Catalog creation failed. Check your parameter values.")
        })?;

        catalog_manager
            .create_catalog(self.plan.clone().into())
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
