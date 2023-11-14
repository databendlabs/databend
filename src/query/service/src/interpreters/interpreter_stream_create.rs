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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_sql::plans::CreateStreamPlan;
use common_sql::plans::StreamNavigation;
use common_storages_factory::Table;
use common_storages_fuse::FuseTable;
use common_storages_fuse::TableContext;
use common_storages_stream::stream_table::StreamTable;
use common_storages_stream::stream_table::MODE_APPEND_ONLY;
use common_storages_stream::stream_table::OPT_KEY_MODE;
use common_storages_stream::stream_table::OPT_KEY_TABLE_ID;
use common_storages_stream::stream_table::OPT_KEY_TABLE_NAME;
use common_storages_stream::stream_table::OPT_KEY_TABLE_VER;
use common_storages_stream::stream_table::STREAM_ENGINE;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateStreamInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateStreamPlan,
}

impl CreateStreamInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateStreamPlan) -> Result<Self> {
        Ok(CreateStreamInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateStreamInterpreter {
    fn name(&self) -> &str {
        "CreateStreamInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let catalog = self.ctx.get_catalog(&plan.catalog).await?;
        let tenant = self.ctx.get_tenant();
        let table = catalog
            .get_table(tenant.as_str(), &plan.table_database, &plan.table_name)
            .await?;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let table_info = fuse_table.get_table_info();
        let table_id = table_info.ident.table_id.to_string();
        let table_version = table_info.ident.seq;
        let schema = table_info.schema().clone();

        let mut options = BTreeMap::new();
        match &plan.navigation {
            Some(StreamNavigation::AtStream { database, name }) => {
                let stream = catalog.get_table(tenant.as_str(), database, name).await?;
                let stream = StreamTable::try_from_table(stream.as_ref())?;
                let stream_opts = stream.get_table_info().options();
                let stream_table_id = stream_opts
                    .get(OPT_KEY_TABLE_ID)
                    .ok_or(ErrorCode::IllegalStream(format!("Illegal stream '{name}'")))?;
                if stream_table_id != &table_id {
                    return Err(ErrorCode::IllegalStream(format!(
                        "The stream '{name}' is not match the table '{}'",
                        plan.table_name
                    )));
                }
                options = stream.get_table_info().options().clone();
            }
            None => {
                options.insert(OPT_KEY_TABLE_NAME.to_string(), plan.table_name.clone());
                options.insert(OPT_KEY_TABLE_ID.to_string(), table_id.to_string());
                options.insert(OPT_KEY_TABLE_VER.to_string(), table_version.to_string());
                options.insert(OPT_KEY_MODE.to_string(), MODE_APPEND_ONLY.to_string());
                if let Some(snapshot_loc) = fuse_table.snapshot_loc().await? {
                    options.insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), snapshot_loc);
                }
            }
        }

        let plan = CreateTableReq {
            if_not_exists: self.plan.if_not_exists,
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.clone(),
                db_name: self.plan.database.clone(),
                table_name: self.plan.stream_name.clone(),
            },
            table_meta: TableMeta {
                engine: STREAM_ENGINE.to_string(),
                options,
                comment: plan.comment.clone().unwrap_or("".to_string()),
                schema,
                ..Default::default()
            },
        };
        catalog.create_table(plan).await?;

        Ok(PipelineBuildResult::create())
    }
}
