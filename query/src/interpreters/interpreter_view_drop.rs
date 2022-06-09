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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::DropTableReq;
use common_meta_app::schema::TableNameIdent;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;
use common_planners::DropViewPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;
use crate::storages::view::view_table::VIEW_ENGINE;

pub struct DropViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropViewPlan,
}

impl DropViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropViewPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(DropViewInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for DropViewInterpreter {
    fn name(&self) -> &str {
        "DropViewInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let viewname = self.plan.viewname.clone();
        let tbl = self
            .ctx
            .get_table(&catalog_name, &db_name, &viewname)
            .await
            .ok();

        self.ctx
            .get_current_session()
            .validate_privilege(
                &GrantObject::Database(catalog_name.clone(), db_name.clone()),
                UserPrivilegeType::Drop,
            )
            .await?;

        if let Some(table) = &tbl {
            if table.get_table_info().engine() != VIEW_ENGINE {
                return Err(ErrorCode::UnexpectedError(format!(
                    "{}.{} is not VIEW, please use `DROP TABLE {}.{}`",
                    &self.plan.database,
                    &self.plan.viewname,
                    &self.plan.database,
                    &self.plan.viewname
                )));
            }
        };

        let catalog = self.ctx.get_catalog(&self.plan.catalog)?;
        let plan = DropTableReq {
            if_exists: self.plan.if_exists,
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.clone(),
                db_name,
                table_name: viewname,
            },
        };
        catalog.drop_table(plan).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
