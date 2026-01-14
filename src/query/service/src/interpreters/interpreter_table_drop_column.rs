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
use databend_common_sql::plans::DropTableColumnPlan;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_storages_common_table_meta::table::OPT_KEY_APPROX_DISTINCT_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;

use crate::interpreters::Interpreter;
use crate::interpreters::common::check_referenced_computed_columns;
use crate::interpreters::common::cluster_key_referenced_columns;
use crate::interpreters::interpreter_table_add_column::commit_table_meta;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTableColumnPlan,
}

impl DropTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTableColumnPlan) -> Result<Self> {
        Ok(DropTableColumnInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableColumnInterpreter {
    fn name(&self) -> &str {
        "DropTableColumnInterpreter"
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

        let table_schema = table.schema();
        let field = table_schema.field_with_name(self.plan.column.as_str())?;

        if let Some((_, cluster_key)) = table.cluster_key_meta() {
            let referenced = cluster_key_referenced_columns(&cluster_key)?;
            if referenced.contains(self.plan.column.as_str()) {
                return Err(ErrorCode::AlterTableError(format!(
                    "Cannot drop column '{}' because it is referenced by cluster key {}",
                    self.plan.column, cluster_key
                )));
            }
        }

        if table_info.meta.is_column_reference_policy(&field.column_id) {
            return Err(ErrorCode::AlterTableError(format!(
                "Cannot drop column '{}' which is associated with a security policy",
                self.plan.column.as_str()
            )));
        }
        if field.computed_expr().is_none() {
            let mut schema: DataSchema = table_schema.as_ref().into();
            schema.drop_column(self.plan.column.as_str())?;
            // Check if this column is referenced by computed columns.
            check_referenced_computed_columns(
                self.ctx.clone(),
                Arc::new(schema),
                self.plan.column.as_str(),
            )?;
        }
        // If the column is table index column, the column can't be dropped.
        if !table_info.meta.indexes.is_empty() {
            for (index_name, index) in &table_info.meta.indexes {
                if index.column_ids.contains(&field.column_id) {
                    return Err(ErrorCode::ColumnReferencedByInvertedIndex(format!(
                        "column `{}` is referenced by inverted index, drop {} index `{}` first",
                        index.index_type, field.name, index_name,
                    )));
                }
            }
        }

        let mut new_table_meta = table_info.meta.clone();
        let new_schema = if self.plan.branch.is_some() {
            let mut new_schema = table_schema.as_ref().clone();
            new_schema.drop_column(&self.plan.column)?;
            new_schema
        } else {
            new_table_meta.drop_column(&self.plan.column)?;
            // update table options
            let opts = &mut new_table_meta.options;
            if let Some(value) = opts.get_mut(OPT_KEY_BLOOM_INDEX_COLUMNS) {
                let bloom_index_cols = value.parse::<BloomIndexColumns>()?;
                if let BloomIndexColumns::Specify(mut cols) = bloom_index_cols {
                    if let Some(pos) = cols.iter().position(|x| *x == self.plan.column) {
                        // remove from the bloom index columns.
                        cols.remove(pos);
                        *value = cols.join(",");
                    }
                }
            }
            if let Some(value) = opts.get_mut(OPT_KEY_APPROX_DISTINCT_COLUMNS) {
                if let ApproxDistinctColumns::Specify(mut cols) =
                    value.parse::<ApproxDistinctColumns>()?
                {
                    if let Some(pos) = cols.iter().position(|x| *x == self.plan.column) {
                        // remove from the approx distinct columns.
                        cols.remove(pos);
                        *value = cols.join(",");
                    }
                }
            }
            new_table_meta.schema.as_ref().clone()
        };

        commit_table_meta(
            &self.ctx,
            table.as_ref(),
            new_table_meta,
            catalog,
            |snapshot_opt, _| {
                if let Some(snapshot) = snapshot_opt {
                    snapshot.schema = new_schema;
                }
            },
        )
        .await?;

        Ok(PipelineBuildResult::create())
    }
}
