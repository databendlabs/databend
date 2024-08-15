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
use databend_common_exception::Result;
use databend_common_meta_api::ShareApi;
use databend_common_meta_app::share::share_name_ident::ShareNameIdent;
use databend_common_meta_app::share::GrantShareObjectReq;
use databend_common_meta_app::share::ShareGrantObjectName;
use databend_common_sql::plans::Plan;
use databend_common_sql::Planner;
use databend_common_storages_share::save_share_spec;
use databend_common_storages_share::update_share_table_info;
use databend_common_storages_view::view_table::QUERY;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::share::GrantShareObjectPlan;

pub struct GrantShareObjectInterpreter {
    ctx: Arc<QueryContext>,
    plan: GrantShareObjectPlan,
}

impl GrantShareObjectInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: GrantShareObjectPlan) -> Result<Self> {
        Ok(GrantShareObjectInterpreter { ctx, plan })
    }

    async fn parse_view_reference_tables(
        &self,
        db_name: &str,
        view: &str,
    ) -> Result<Vec<(String, String)>> {
        // first get view table meta
        let catalog = self.ctx.get_default_catalog()?;
        let reply = catalog
            .get_table(&self.ctx.get_tenant(), db_name, view)
            .await?;
        let table_meta = &reply.get_table_info().meta;

        // parse subquery in view, get all reference tables
        let mut reference_tables = vec![];
        if let Some(query) = table_meta.options.get(QUERY) {
            let mut planner = Planner::new(self.ctx.clone());
            let (plan, _) = planner.plan_sql(query).await?;

            if let Plan::Query { metadata, .. } = plan {
                let metadata = metadata.read().clone();
                for table in metadata.tables() {
                    let database_name = table.database();
                    let table_name = table.name();
                    reference_tables.push((database_name.to_string(), table_name.to_string()));
                }
            }
        }
        Ok(reference_tables)
    }
}

#[async_trait::async_trait]
impl Interpreter for GrantShareObjectInterpreter {
    fn name(&self) -> &str {
        "GrantShareObjectInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let reference_tables = if let ShareGrantObjectName::View(db, view) = &self.plan.object {
            Some(self.parse_view_reference_tables(db, view).await?)
        } else {
            None
        };

        let req = GrantShareObjectReq {
            share_name: ShareNameIdent::new(&tenant, &self.plan.share),
            object: self.plan.object.clone(),
            privilege: self.plan.privilege,
            grant_on: Utc::now(),
            reference_tables,
        };
        let resp = meta_api.grant_share_object(req).await?;

        if let Some(share_spec) = &resp.share_spec {
            save_share_spec(
                self.ctx.get_tenant().tenant_name(),
                self.ctx.get_application_level_data_operator()?.operator(),
                &[share_spec.clone()],
            )
            .await?;

            // if grant object is table, save table info
            if let Some((db_id, share_table_info)) = &resp.grant_share_table {
                update_share_table_info(
                    self.ctx.get_tenant().tenant_name(),
                    self.ctx.get_application_level_data_operator()?.operator(),
                    &[share_spec.name.clone()],
                    *db_id,
                    share_table_info,
                )
                .await?;
            }
            // save reference table info, if any
            if let Some(reference_tables) = &resp.reference_tables {
                for (db_id, ref_table) in reference_tables {
                    update_share_table_info(
                        self.ctx.get_tenant().tenant_name(),
                        self.ctx.get_application_level_data_operator()?.operator(),
                        &[share_spec.name.clone()],
                        *db_id,
                        ref_table,
                    )
                    .await?;
                }
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
