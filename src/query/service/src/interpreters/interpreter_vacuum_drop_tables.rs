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

use common_exception::Result;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FromData;
use common_license::license::Feature::Vacuum;
use common_license::license_manager::get_license_manager;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::GcDroppedTableReq;
use common_meta_app::schema::ListDroppedTableReq;
use common_meta_app::schema::TableInfoFilter;
use common_sql::plans::VacuumDropTablePlan;
use vacuum_handler::get_vacuum_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

const DRY_RUN_LIMIT: usize = 1000;

#[allow(dead_code)]
pub struct VacuumDropTablesInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumDropTablePlan,
}

impl VacuumDropTablesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumDropTablePlan) -> Result<Self> {
        Ok(VacuumDropTablesInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumDropTablesInterpreter {
    fn name(&self) -> &str {
        "VacuumDropTablesInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager.manager.check_enterprise_enabled(
            &self.ctx.get_settings(),
            self.ctx.get_tenant(),
            Vacuum,
        )?;

        let ctx = self.ctx.clone();
        let hours = match self.plan.option.retain_hours {
            Some(hours) => hours as i64,
            None => ctx.get_settings().get_retention_period()? as i64,
        };
        let retention_time = chrono::Utc::now() - chrono::Duration::hours(hours);
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str())?;
        // if database if empty, vacuum all tables
        let filter = if self.plan.database.is_empty() {
            TableInfoFilter::AllDroppedTables(Some(retention_time))
        } else {
            TableInfoFilter::Dropped(Some(retention_time))
        };

        let tenant = self.ctx.get_tenant();
        let (tables, drop_ids) = catalog
            .get_drop_table_infos(ListDroppedTableReq {
                inner: DatabaseNameIdent {
                    tenant,
                    db_name: self.plan.database.clone(),
                },
                filter,
            })
            .await?;
        let handler = get_vacuum_handler();
        let files_opt = handler
            .do_vacuum_drop_tables(
                tables,
                if self.plan.option.dry_run.is_some() {
                    Some(DRY_RUN_LIMIT)
                } else {
                    None
                },
            )
            .await?;
        // gc meta data when not dry run
        if self.plan.option.dry_run.is_none() {
            let req = GcDroppedTableReq {
                tenant: self.ctx.get_tenant(),
                drop_ids,
            };
            let _ = catalog.gc_drop_tables(req).await?;
        }

        match files_opt {
            None => return Ok(PipelineBuildResult::create()),
            Some(purge_files) => {
                let len = min(purge_files.len(), DRY_RUN_LIMIT);
                let mut tables: Vec<Vec<u8>> = Vec::with_capacity(len);
                let mut files: Vec<Vec<u8>> = Vec::with_capacity(len);
                let purge_files = &purge_files[0..len];
                for file in purge_files.iter() {
                    tables.push(file.0.to_string().as_bytes().to_vec());
                    files.push(file.1.to_string().as_bytes().to_vec());
                }

                PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                    StringType::from_data(tables),
                    StringType::from_data(files),
                ])])
            }
        }
    }
}
