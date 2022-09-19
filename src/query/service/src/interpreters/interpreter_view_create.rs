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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::CreateViewPlan;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::view::view_table::VIEW_ENGINE;

pub struct CreateViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateViewPlan,
}

impl CreateViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateViewPlan) -> Result<Self> {
        Ok(CreateViewInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateViewInterpreter {
    fn name(&self) -> &str {
        "CreateViewInterpreter"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // check whether view has exists
        if self
            .ctx
            .get_catalog(&self.plan.catalog)?
            .list_tables(&*self.plan.tenant, &*self.plan.database)
            .await?
            .iter()
            .any(|table| table.name() == self.plan.viewname.as_str())
        {
            return Err(ErrorCode::ViewAlreadyExists(format!(
                "{}.{} as view Already Exists",
                self.plan.database, self.plan.viewname
            )));
        }

        self.create_view().await
    }
}

impl CreateViewInterpreter {
    async fn create_view(&self) -> Result<PipelineBuildResult> {
        let catalog = self.ctx.get_catalog(&self.plan.catalog)?;
        let mut options = BTreeMap::new();
        options.insert("query".to_string(), self.plan.subquery.clone());
        let plan = CreateTableReq {
            if_not_exists: self.plan.if_not_exists,
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.clone(),
                db_name: self.plan.database.clone(),
                table_name: self.plan.viewname.clone(),
            },
            table_meta: TableMeta {
                engine: VIEW_ENGINE.to_string(),
                options,
                ..Default::default()
            },
        };
        catalog.create_table(plan).await?;

        Ok(PipelineBuildResult::create())
    }
}
