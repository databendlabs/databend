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

use databend_common_ast::ast::UriLocation;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_sql::binder::parse_storage_params_from_uri;
use databend_common_sql::plans::ModifyTableConnectionPlan;
use databend_common_storage::check_operator;
use databend_common_storage::init_operator;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use log::debug;

use crate::interpreters::Interpreter;
use crate::interpreters::interpreter_table_add_column::commit_table_meta;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ModifyTableConnectionInterpreter {
    ctx: Arc<QueryContext>,
    plan: ModifyTableConnectionPlan,
}

impl ModifyTableConnectionInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ModifyTableConnectionPlan) -> Result<Self> {
        Ok(ModifyTableConnectionInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ModifyTableConnectionInterpreter {
    fn name(&self) -> &str {
        "ModifyTableConnectionInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let table = self
            .ctx
            .get_catalog(catalog_name)
            .await?
            .get_table(&self.ctx.get_tenant(), db_name, tbl_name)
            .await?;

        // check mutability
        table.check_mutable()?;

        let table_info = table.get_table_info();
        let engine = table.engine();
        if matches!(engine, VIEW_ENGINE | STREAM_ENGINE) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is {} that doesn't support alter",
                &self.plan.database, &self.plan.table, engine
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }
        let Some(old_sp) = table_info.meta.storage_params.clone() else {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} is not an external table, cannot alter connection",
                &self.plan.database, &self.plan.table
            )));
        };

        debug!("old storage params before update: {old_sp:?}");

        // This location is used to parse the storage parameters from the URI.
        //
        // We don't really this this location to replace the old one, we just parse it out and change the storage parameters on needs.
        let mut location = UriLocation::new(
            // The storage type is not changeable, we just use the old one.
            old_sp.storage_type().to_string(),
            // name is not changeable, we just use a dummy value here.
            "test".to_string(),
            // root is not changeable, we just use a dummy value here.
            "/".to_string(),
            self.plan.new_connection.clone(),
        );
        // NOTE: never use this storage params directly.
        let updated_sp = parse_storage_params_from_uri(
            &mut location,
            Some(self.ctx.as_ref() as _),
            "when ALTER TABLE CONNECTION",
        )
        .await?;

        debug!("storage params used for update: {updated_sp:?}");
        let new_sp = old_sp.apply_update(updated_sp)?;
        debug!("new storage params been updated: {new_sp:?}");

        // Check the storage params via init operator.
        let op = init_operator(&new_sp).map_err(|err| {
            ErrorCode::InvalidConfig(format!(
                "Input storage config for stage is invalid: {err:?}"
            ))
        })?;
        check_operator(&op, &new_sp).await?;

        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        let mut new_table_meta = table_info.meta.clone();
        new_table_meta.storage_params = Some(new_sp);

        commit_table_meta(
            &self.ctx,
            table.as_ref(),
            new_table_meta,
            catalog,
            |_, _| {},
        )
        .await?;

        Ok(PipelineBuildResult::create())
    }
}
