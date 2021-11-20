// Copyright 2020 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::GrantPrivilegePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::catalogs::impls::DatabaseCatalog;
use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::DatabendQueryContextRef;

#[derive(Debug)]
pub struct GrantPrivilegeInterpreter {
    ctx: DatabendQueryContextRef,
    plan: GrantPrivilegePlan,
}

impl GrantPrivilegeInterpreter {
    pub fn try_create(
        ctx: DatabendQueryContextRef,
        plan: GrantPrivilegePlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(GrantPrivilegeInterpreter { ctx, plan }))
    }

    // TODO: refactor out this after adding a exists table in Catalog
    async fn check_table_exists(
        &self,
        catalog: Arc<DatabaseCatalog>,
        database_name: &str,
        table_name: &str,
    ) -> Result<bool> {
        match catalog.get_table(database_name, table_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UnknownTableCode() {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for GrantPrivilegeInterpreter {
    fn name(&self) -> &str {
        "GrantPrivilegeInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();

        // *.`table` is not allowed
        if plan.database_pattern == "*" && plan.table_pattern != "*" {
            return Err(common_exception::ErrorCode::UnknownTable(format!(
                "can not grant privilege on *.{}",
                plan.table_pattern
            )));
        }

        // check database & table existence
        let catalog = self.ctx.get_catalog();
        if plan.database_pattern != "*"
            && !catalog
                .as_ref()
                .exists_database(&plan.database_pattern)
                .await?
        {
            return Err(common_exception::ErrorCode::UnknownDatabase(format!(
                "database {} not exists",
                plan.database_pattern
            )));
        }

        if plan.table_pattern != "*"
            && !self
                .check_table_exists(catalog.clone(), &plan.database_pattern, &plan.table_pattern)
                .await?
        {
            return Err(common_exception::ErrorCode::UnknownTable(format!(
                "table {}.{} not exists",
                plan.database_pattern, plan.table_pattern
            )));
        }

        let user_mgr = self.ctx.get_sessions_manager().get_user_manager();
        user_mgr
            .set_user_privileges(&plan.name, &plan.hostname, plan.priv_types)
            .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
