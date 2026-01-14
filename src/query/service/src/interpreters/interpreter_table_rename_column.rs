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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_sql::ApproxDistinctColumns;
use databend_common_sql::BloomIndexColumns;
use databend_common_sql::plans::RenameTableColumnPlan;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_iceberg::table::ICEBERG_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_storages_common_table_meta::table::OPT_KEY_APPROX_DISTINCT_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;

use crate::interpreters::Interpreter;
use crate::interpreters::common::check_referenced_computed_columns;
use crate::interpreters::interpreter_table_add_column::commit_table_meta;
use crate::interpreters::interpreter_table_create::is_valid_column;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
pub struct RenameTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: RenameTableColumnPlan,
}

impl RenameTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RenameTableColumnPlan) -> Result<Self> {
        Ok(RenameTableColumnInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RenameTableColumnInterpreter {
    fn name(&self) -> &str {
        "RenameTableColumnInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let table = catalog
            .get_table_with_batch(
                &self.ctx.get_tenant(),
                db_name,
                tbl_name,
                self.plan.branch.as_deref(),
            )
            .await?;

        // check mutability
        table.check_mutable()?;

        let table_info = table.get_table_info();
        let engine = table.engine();
        if matches!(
            engine.to_uppercase().as_str(),
            VIEW_ENGINE | STREAM_ENGINE | ICEBERG_ENGINE
        ) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is {} that doesn't support rename column name",
                &self.plan.database, &self.plan.table, engine
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }

        let mut new_table_meta = table.get_table_info().meta.clone();

        is_valid_column(&self.plan.new_column)?;

        let mut schema: DataSchema = table.schema().into();
        let field = schema.field_with_name(self.plan.old_column.as_str())?;
        if field.computed_expr().is_none() {
            let index = schema.index_of(self.plan.old_column.as_str())?;
            schema.rename_field(index, self.plan.new_column.as_str());
            // Check if old column is referenced by computed columns.
            check_referenced_computed_columns(
                self.ctx.clone(),
                Arc::new(schema),
                self.plan.old_column.as_str(),
            )?;
        }

        // update table options
        if self.plan.branch.is_none() {
            let opts = &mut new_table_meta.options;
            if let Some(value) = opts.get_mut(OPT_KEY_BLOOM_INDEX_COLUMNS) {
                let bloom_index_cols = value.parse::<BloomIndexColumns>()?;
                if let BloomIndexColumns::Specify(mut cols) = bloom_index_cols {
                    if let Some(pos) = cols.iter().position(|x| *x == self.plan.old_column) {
                        // replace the bloom index columns with new column name.
                        cols[pos] = self.plan.new_column.clone();
                        *value = cols.join(",");
                    }
                }
            }
            if let Some(value) = opts.get_mut(OPT_KEY_APPROX_DISTINCT_COLUMNS) {
                let approx_distinct_cols = value.parse::<ApproxDistinctColumns>()?;
                if let ApproxDistinctColumns::Specify(mut cols) = approx_distinct_cols {
                    if let Some(pos) = cols.iter().position(|x| *x == self.plan.old_column) {
                        // replace the approx distinct columns with new column name.
                        cols[pos] = self.plan.new_column.clone();
                        *value = cols.join(",");
                    }
                }
            }
        }

        commit_table_meta(
            &self.ctx,
            table.as_ref(),
            Arc::new(self.plan.schema.clone()),
            new_table_meta,
            catalog,
        )
        .await?;

        Ok(PipelineBuildResult::create())
    }
}
