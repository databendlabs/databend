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

use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_sql::plans::DropTableConstraintPlan;
use databend_common_storages_fuse::FuseTable;

use crate::interpreters::Interpreter;
use crate::interpreters::interpreter_table_add_column::update_table_meta;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Clone, Debug)]
pub struct DropTableConstraintInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTableConstraintPlan,
}

impl DropTableConstraintInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTableConstraintPlan) -> Result<Self> {
        Ok(DropTableConstraintInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableConstraintInterpreter {
    fn name(&self) -> &str {
        "DropTableConstraintInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;
        // check mutability
        tbl.check_mutable()?;

        let table_info = tbl.get_table_info();
        let engine = table_info.engine();
        let fuse_table = FuseTable::try_from_table(tbl.as_ref()).map_err(|_| {
            ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is {} that doesn't support alter",
                &self.plan.database, &self.plan.table, engine
            ))
        })?;
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }

        let mut new_table_meta = table_info.meta.clone();
        new_table_meta.drop_constraint(&self.plan.constraint_name)?;

        let catalog = self.ctx.get_catalog(catalog_name).await?;
        update_table_meta(fuse_table, &new_table_meta, catalog).await?;
        Ok(PipelineBuildResult::create())
    }
}
