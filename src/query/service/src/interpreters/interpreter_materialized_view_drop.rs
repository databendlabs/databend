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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_ENGINE;
use databend_common_sql::plans::DropMaterializedViewPlan;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextTableAccess;

pub struct DropMaterializedViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropMaterializedViewPlan,
}

impl DropMaterializedViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropMaterializedViewPlan) -> Result<Self> {
        Ok(DropMaterializedViewInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropMaterializedViewInterpreter {
    fn name(&self) -> &str {
        "DropMaterializedViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::MaterializedView)?;

        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let view_name = self.plan.view_name.clone();
        let tbl = self
            .ctx
            .get_table(&catalog_name, &db_name, &view_name)
            .await
            .ok();

        if tbl.is_none() && !self.plan.if_exists {
            return Err(ErrorCode::UnknownTable(format!(
                "unknown materialized view `{}`.`{}` in catalog '{}'",
                db_name, view_name, &catalog_name
            )));
        }

        if let Some(table) = &tbl {
            let engine = table.get_table_info().engine();
            if engine != MATERIALIZED_VIEW_ENGINE {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "{}.{} is not MATERIALIZED VIEW, please use `DROP {} {}.{}`",
                    &self.plan.database,
                    &self.plan.view_name,
                    if engine == STREAM_ENGINE {
                        "STREAM"
                    } else if engine == VIEW_ENGINE {
                        "VIEW"
                    } else {
                        "TABLE"
                    },
                    &self.plan.database,
                    &self.plan.view_name
                )));
            }

            let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
            let db = catalog
                .get_database(&self.plan.tenant, &self.plan.database)
                .await?;
            catalog
                .drop_table_by_id(DropTableByIdReq {
                    if_exists: self.plan.if_exists,
                    tenant: self.plan.tenant.clone(),
                    table_name: self.plan.view_name.clone(),
                    tb_id: table.get_id(),
                    db_id: db.get_db_info().database_id.db_id,
                    db_name: db.name().to_string(),
                    engine: table.engine().to_string(),
                    temp_prefix: table
                        .options()
                        .get(OPT_KEY_TEMP_PREFIX)
                        .cloned()
                        .unwrap_or_default(),
                })
                .await?;
        };

        Ok(PipelineBuildResult::create())
    }
}
