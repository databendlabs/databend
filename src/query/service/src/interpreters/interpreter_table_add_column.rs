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
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ComputedExpr;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_sql::DefaultExprBinder;
use databend_common_sql::Planner;
use databend_common_sql::plans::AddColumnOption;
use databend_common_sql::plans::AddTableColumnPlan;
use databend_common_sql::plans::Mutation;
use databend_common_sql::plans::Plan;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_meta_types::MatchSeq;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use log::info;

use crate::interpreters::Interpreter;
use crate::interpreters::MutationInterpreter;
use crate::interpreters::interpreter_table_create::is_valid_column;
use crate::interpreters::interpreter_table_modify_column::build_select_insert_plan;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct AddTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: AddTableColumnPlan,
}

impl AddTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AddTableColumnPlan) -> Result<Self> {
        Ok(AddTableColumnInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AddTableColumnInterpreter {
    fn name(&self) -> &str {
        "AddTableColumnInterpreter"
    }

    fn is_ddl(&self) -> bool {
        self.plan.is_deterministic
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let tbl = catalog
            .get_table_with_branch(
                &self.ctx.get_tenant(),
                db_name,
                tbl_name,
                self.plan.branch.as_deref(),
            )
            .await?;
        // check mutability
        tbl.check_mutable()?;

        let mut table_info = tbl.get_table_info().clone();
        let engine = table_info.engine();
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

        let field = self.plan.field.clone();
        if field.computed_expr().is_some() {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.ctx.get_license_key(), ComputedColumn)?;
        }

        let num_rows = if self.plan.branch.is_some() {
            let fuse_tbl = FuseTable::try_from_table(tbl.as_ref())?;
            fuse_tbl
                .read_table_snapshot()
                .await?
                .map(|v| v.summary.row_count)
                .unwrap_or_default()
        } else {
            table_info.meta.statistics.number_of_rows
        };
        if self.plan.is_nextval && num_rows > 0 {
            return Err(ErrorCode::AlterTableError(format!(
                "Cannot add column '{}' with `nextval` as default value to non-empty table '{}'",
                &self.plan.field.name, &self.plan.table
            )));
        }
        if self.plan.is_autoincrement && num_rows > 0 {
            return Err(ErrorCode::AlterTableError(format!(
                "Cannot add column '{}' with `AUTOINCREMENT` to non-empty table '{}'",
                &self.plan.field.name, &self.plan.table
            )));
        }
        if field.default_expr().is_some() {
            let _ = DefaultExprBinder::try_new(self.ctx.clone())?.get_scalar(&field)?;
        }
        is_valid_column(field.name())?;
        let schema = tbl.schema();
        let index = match &self.plan.option {
            AddColumnOption::First => 0,
            AddColumnOption::After(name) => schema.index_of(name)? + 1,
            AddColumnOption::End => schema.num_fields(),
        };
        let new_schema = if self.plan.branch.is_some() {
            let mut new_schema = schema.as_ref().clone();
            new_schema.add_column(&field, index)?;
            Arc::new(new_schema)
        } else {
            table_info
                .meta
                .add_column(&field, &self.plan.comment, index)?;
            table_info.meta.schema.clone()
        };

        // if the new column is a stored computed field and table is non-empty,
        // need rebuild the table to generate stored computed column.
        if num_rows > 0 && matches!(field.computed_expr, Some(ComputedExpr::Stored(_))) {
            let fuse_table = FuseTable::try_from_table(tbl.as_ref())?;
            if fuse_table.change_tracking_enabled() {
                return Err(ErrorCode::AlterTableError(format!(
                    "Cannot add stored computed column to table '{}' with change tracking enabled",
                    table_info.desc
                )));
            }
            let base_snapshot = fuse_table.read_table_snapshot().await?;
            let prev_snapshot_id = base_snapshot.snapshot_id().map(|(id, _)| id);
            let table_meta_timestamps = self
                .ctx
                .get_table_meta_timestamps(tbl.as_ref(), base_snapshot)?;

            // computed columns will generated from other columns.
            let new_schema_without_computed_fields = new_schema.remove_computed_fields();
            let query_fields = new_schema_without_computed_fields
                .fields()
                .iter()
                .map(|field| format!("`{}`", field.name))
                .collect::<Vec<_>>()
                .join(", ");

            let table_ref = if let Some(branch) = &self.plan.branch {
                format!(
                    "`{}`.`{}`/`{}`",
                    self.plan.database, self.plan.table, branch
                )
            } else {
                format!("`{}`.`{}`", self.plan.database, self.plan.table)
            };

            let sql = format!("SELECT {} FROM {}", query_fields, table_ref);
            let mut branch_info = fuse_table.get_branch_info().cloned();
            if let Some(inner) = branch_info.as_mut() {
                inner.schema = new_schema;
            }
            return build_select_insert_plan(
                self.ctx.clone(),
                sql,
                table_info.clone(),
                branch_info,
                new_schema_without_computed_fields.into(),
                prev_snapshot_id,
                table_meta_timestamps,
            )
            .await;
        }

        let need_update = num_rows > 0 && !self.plan.is_deterministic;
        if self.plan.branch.is_some() && need_update {
            return Err(ErrorCode::AlterTableError(format!(
                "Cannot add non-deterministic default column to branch table '{}', UPDATE is not supported on branch tables yet",
                table_info.desc
            )));
        }
        if need_update && tbl.change_tracking_enabled() {
            // Rebuild table while change tracking is active may break the consistency
            // of tracked changes, leading to incorrect change records.
            return Err(ErrorCode::AlterTableError(format!(
                "Cannot add non-deterministic default column to table '{}' with change tracking enabled",
                table_info.desc
            )));
        }

        commit_table_meta(
            &self.ctx,
            tbl.as_ref(),
            table_info.meta.clone(),
            catalog,
            |snapshot_opt, _| {
                if let Some(snapshot) = snapshot_opt {
                    snapshot.schema = new_schema.as_ref().clone();
                }
            },
        )
        .await?;

        // If the column is not deterministic and table is non-empty,
        // update to refresh the value with default expr.
        if need_update {
            self.ctx
                .evict_table_from_cache(catalog_name, db_name, tbl_name)?;
            let query = format!(
                "UPDATE `{}`.`{}` SET `{}` = {};",
                db_name,
                tbl_name,
                field.name(),
                field.default_expr().unwrap()
            );
            let mut planner = Planner::new(self.ctx.clone());
            let (plan, _) = planner.plan_sql(&query).await?;
            if let Plan::DataMutation { s_expr, schema, .. } = plan {
                let mutation: Mutation = s_expr.plan().clone().try_into()?;
                let interpreter = MutationInterpreter::try_create(
                    self.ctx.clone(),
                    *s_expr,
                    schema,
                    mutation.metadata.clone(),
                )?;
                let _ = interpreter.execute(self.ctx.clone()).await?;
                return Ok(PipelineBuildResult::create());
            }
        }
        Ok(PipelineBuildResult::create())
    }
}

pub(crate) async fn commit_table_meta<F>(
    ctx: &QueryContext,
    tbl: &dyn Table,
    mut new_table_meta: TableMeta,
    catalog: Arc<dyn Catalog>,
    f: F,
) -> Result<()>
where
    F: FnOnce(&mut Option<TableSnapshot>, &mut TableMeta),
{
    if let Ok(fuse_tbl) = FuseTable::try_from_table(tbl) {
        let mut new_snapshot = if let Some(prev) = fuse_tbl.read_table_snapshot().await? {
            // Build new snapshot from previous
            Some(TableSnapshot::try_from_previous(
                prev.clone(),
                Some(fuse_tbl.get_table_info().ident.seq),
                ctx.get_table_meta_timestamps(fuse_tbl, Some(prev.clone()))?,
            )?)
        } else {
            info!("Snapshot not found, no need to generate new snapshot");
            None
        };

        f(&mut new_snapshot, &mut new_table_meta);

        let mut new_snapshot_location = None;
        if let Some(new_snapshot) = new_snapshot {
            // Persist the snapshot
            let new_snapshot_loc = fuse_tbl.meta_location_generator().gen_snapshot_location(
                fuse_tbl.get_branch_id(),
                &new_snapshot.snapshot_id,
                TableSnapshot::VERSION,
            )?;

            let data = new_snapshot.to_bytes()?;
            fuse_tbl
                .get_operator_ref()
                .write(&new_snapshot_loc, data)
                .await?;
            new_snapshot_location = Some(new_snapshot_loc);
        }

        if let Some(branch_name) = fuse_tbl.get_branch_name() {
            // No need to write snapshot hint for table branch.
            if let Some(new_snapshot_location) = new_snapshot_location.take() {
                if let Some(branch_ref) = new_table_meta.refs.get_mut(branch_name) {
                    branch_ref.loc = new_snapshot_location;
                }
            } else {
                return Err(ErrorCode::IllegalReference(format!(
                    "Cannot read the snapshot from table: {}, branch {}",
                    fuse_tbl.get_table_info().desc,
                    branch_name,
                )));
            }
        } else if let Some(new_snapshot_location) = &new_snapshot_location {
            new_table_meta.options.insert(
                OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
                new_snapshot_location.clone(),
            );
        }
        new_table_meta.updated_on = Utc::now();

        update_table_meta(fuse_tbl, &new_table_meta, catalog).await?;

        if let Some(new_snapshot_location) = new_snapshot_location {
            FuseTable::write_last_snapshot_hint(
                ctx,
                fuse_tbl.get_operator_ref(),
                fuse_tbl.meta_location_generator(),
                &new_snapshot_location,
                &new_table_meta,
            )
            .await;
        }
    }
    Ok(())
}

pub(crate) async fn update_table_meta(
    fuse_tbl: &FuseTable,
    new_table_meta: &TableMeta,
    catalog: Arc<dyn Catalog>,
) -> Result<()> {
    let mut table_info = fuse_tbl.get_table_info().clone();
    let table_id = table_info.ident.table_id;
    let table_version = table_info.ident.seq;
    let req = UpdateTableMetaReq {
        table_id,
        seq: MatchSeq::Exact(table_version),
        new_table_meta: new_table_meta.clone(),
        base_snapshot_location: fuse_tbl.snapshot_loc(),
        lvt_check: None,
    };
    table_info.meta = new_table_meta.clone();
    catalog.update_single_table_meta(req, &table_info).await?;
    Ok(())
}
