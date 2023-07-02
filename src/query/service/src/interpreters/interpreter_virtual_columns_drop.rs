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

use common_exception::Result;
use common_expression::DataSchemaRef;
use common_license::license::Feature::VirtualColumns;
use common_license::license_manager::get_license_manager;
use common_meta_app::schema::DropVirtualColumnReq;
use common_meta_app::schema::VirtualColumnNameIdent;
use common_sql::plans::DropVirtualColumnsPlan;
use virtual_columns_handler::get_virtual_columns_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropVirtualColumnsInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropVirtualColumnsPlan,
}

impl DropVirtualColumnsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropVirtualColumnsPlan) -> Result<Self> {
        Ok(DropVirtualColumnsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropVirtualColumnsInterpreter {
    fn name(&self) -> &str {
        "DropVirtualColumnsInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let license_manager = get_license_manager();
        license_manager.manager.check_enterprise_enabled(
            &self.ctx.get_settings(),
            tenant.clone(),
            VirtualColumns,
        )?;

        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let tbl_name = self.plan.table.clone();
        let table = self
            .ctx
            .get_table(&catalog_name, &db_name, &tbl_name)
            .await?;

        let table_id = table.get_id();
        let catalog = self.ctx.get_catalog(&catalog_name)?;

        let drop_virtual_column_req = DropVirtualColumnReq {
            name_ident: VirtualColumnNameIdent { tenant, table_id },
        };

        let handler = get_virtual_columns_handler();
        let _ = handler
            .do_drop_virtual_column(catalog, drop_virtual_column_req)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
