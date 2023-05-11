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

use std::cmp::min;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FromData;
use common_license::license_manager::get_license_manager;
use common_sql::plans::VacuumTablePlan;
use common_storages_fuse::FuseTable;
use vacuum_handler::get_vacuum_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

const DRY_RUN_LIMIT: usize = 1000;

#[allow(dead_code)]
pub struct VacuumTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumTablePlan,
}

impl VacuumTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumTablePlan) -> Result<Self> {
        Ok(VacuumTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumTableInterpreter {
    fn name(&self) -> &str {
        "VacuumTableInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        if !license_manager.manager.is_active() {
            return Err(ErrorCode::LicenceDenied(
                "Need Commercial License".to_string(),
            ));
        }

        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let tbl_name = self.plan.table.clone();
        let ctx = self.ctx.clone();
        let table = self
            .ctx
            .get_table(&catalog_name, &db_name, &tbl_name)
            .await?;
        let hours = match self.plan.option.retain_hours {
            Some(hours) => hours as i64,
            None => ctx.get_settings().get_retention_period()? as i64,
        };
        let retention_time = chrono::Utc::now() - chrono::Duration::hours(hours);
        let ctx = self.ctx.clone();

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let handler = get_vacuum_handler();
        let purge_files_opt = handler
            .do_vacuum(
                fuse_table,
                ctx,
                retention_time,
                if self.plan.option.dry_run.is_some() {
                    Some(DRY_RUN_LIMIT)
                } else {
                    None
                },
            )
            .await?;

        match purge_files_opt {
            None => return Ok(PipelineBuildResult::create()),
            Some(purge_files) => {
                let len = min(purge_files.len(), DRY_RUN_LIMIT);
                let mut files: Vec<Vec<u8>> = Vec::with_capacity(len);
                let purge_files = &purge_files[0..len];
                for file in purge_files.iter() {
                    files.push(file.to_string().as_bytes().to_vec());
                }

                PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                    StringType::from_data(files),
                ])])
            }
        }
    }
}
