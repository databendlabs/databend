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

use databend_common_ast::ast::quote::display_ident;
use databend_common_ast::ast::quote::QuotedString;
use databend_common_ast::parser::Dialect;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_sql::plans::ShowCreateTablePlan;
use databend_common_storages_fuse::FUSE_OPT_KEY_ATTACH_COLUMN_IDS;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::QUERY;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use databend_storages_common_table_meta::table::is_internal_opt_key;
use databend_storages_common_table_meta::table::StreamMode;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_PREFIX;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_DATA_URI;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use itertools::Itertools;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ShowCreateTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowCreateTablePlan,
}

pub struct ShowCreateQuerySettings {
    pub sql_dialect: Dialect,
    pub force_quoted_ident: bool,
    pub quoted_ident_case_sensitive: bool,
    pub hide_options_in_show_create_table: bool,
}

impl ShowCreateTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowCreateTablePlan) -> Result<Self> {
        Ok(ShowCreateTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowCreateTableInterpreter {
    fn name(&self) -> &str {
        "ShowCreateTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;

        let table = catalog
            .get_table(&tenant, &self.plan.database, &self.plan.table)
            .await?;

        let settings = self.ctx.get_settings();

        let settings = ShowCreateQuerySettings {
            sql_dialect: settings.get_sql_dialect()?,
            force_quoted_ident: self.plan.with_quoted_ident,
            quoted_ident_case_sensitive: settings.get_quoted_ident_case_sensitive()?,
            hide_options_in_show_create_table: settings
                .get_hide_options_in_show_create_table()
                .unwrap_or(false),
        };

        let create_query = Self::show_create_query(
            catalog.as_ref(),
            &self.plan.database,
            table.as_ref(),
            &settings,
        )
        .await?;

        let block = DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(table.name().to_string())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(create_query)),
                ),
            ],
            1,
        );

        PipelineBuildResult::from_blocks(vec![block])
    }
}

impl ShowCreateTableInterpreter {
    pub async fn show_create_query(
        catalog: &dyn Catalog,
        database: &str,
        table: &dyn Table,
        settings: &ShowCreateQuerySettings,
    ) -> Result<String> {
        match table.engine() {
            STREAM_ENGINE => Self::show_create_stream_query(catalog, table).await,
            VIEW_ENGINE => Self::show_create_view_query(table, database),
            _ => match table.options().get(OPT_KEY_STORAGE_PREFIX) {
                Some(_) => Ok(Self::show_attach_table_query(table, database)),
                None => Self::show_create_table_query(table.get_table_info(), settings),
            },
        }
    }

    pub fn show_create_table_query(
        table_info: &TableInfo,
        settings: &ShowCreateQuerySettings,
    ) -> Result<String> {
        let name = &table_info.name;
        let engine = table_info.engine();
        let schema = table_info.schema();
        let field_comments = table_info.field_comments();
        let options = table_info.options();
        let sql_dialect = settings.sql_dialect;
        let force_quoted_ident = settings.force_quoted_ident;
        let quoted_ident_case_sensitive = settings.quoted_ident_case_sensitive;
        let hide_options_in_show_create_table = settings.hide_options_in_show_create_table;

        let mut table_create_sql = format!(
            "CREATE TABLE {} (\n",
            display_ident(
                name,
                force_quoted_ident,
                quoted_ident_case_sensitive,
                sql_dialect
            )
        );

        if options.contains_key("TRANSIENT") {
            table_create_sql = format!(
                "CREATE TRANSIENT TABLE {} (\n",
                display_ident(
                    name,
                    force_quoted_ident,
                    quoted_ident_case_sensitive,
                    sql_dialect
                )
            )
        }

        if options.contains_key(OPT_KEY_TEMP_PREFIX) {
            table_create_sql = format!(
                "CREATE TEMP TABLE {} (\n",
                display_ident(
                    name,
                    force_quoted_ident,
                    quoted_ident_case_sensitive,
                    sql_dialect
                )
            )
        }

        // Append columns and indexes.
        {
            let mut create_defs = vec![];
            for (idx, field) in schema.fields().iter().enumerate() {
                let default_expr = match field.default_expr() {
                    Some(expr) => {
                        format!(" DEFAULT {expr}")
                    }
                    None => "".to_string(),
                };
                let computed_expr = match field.computed_expr() {
                    Some(ComputedExpr::Virtual(expr)) => {
                        format!(" AS ({expr}) VIRTUAL")
                    }
                    Some(ComputedExpr::Stored(expr)) => {
                        format!(" AS ({expr}) STORED")
                    }
                    _ => "".to_string(),
                };
                let comment = if field_comments.len() == schema.fields().len()
                    && !field_comments[idx].is_empty()
                {
                    format!(" COMMENT {}", QuotedString(&field_comments[idx], '\''))
                } else {
                    "".to_string()
                };

                let ident = display_ident(
                    field.name(),
                    force_quoted_ident,
                    quoted_ident_case_sensitive,
                    sql_dialect,
                );
                let data_type = field.data_type().sql_name_explicit_null();
                let column_str =
                    format!("  {ident} {data_type}{default_expr}{computed_expr}{comment}");

                create_defs.push(column_str);
            }

            for index_field in table_info.meta.indexes.values() {
                let sync = if index_field.sync_creation {
                    "SYNC"
                } else {
                    "ASYNC"
                };
                let mut column_names = Vec::with_capacity(index_field.column_ids.len());
                for column_id in index_field.column_ids.iter() {
                    let field = schema.field_of_column_id(*column_id)?;
                    column_names.push(field.name().to_string());
                }
                let column_names_str = column_names.join(", ").to_string();
                let mut options = Vec::with_capacity(index_field.options.len());
                for (key, value) in index_field.options.iter() {
                    let option = format!("{} = '{}'", key, value);
                    options.push(option);
                }
                let index_type = match index_field.index_type {
                    TableIndexType::Inverted => "INVERTED",
                    TableIndexType::Ngram => "NGRAM",
                    TableIndexType::Vector => "VECTOR",
                };
                let mut index_str = format!(
                    "  {} {} INDEX {} ({})",
                    sync,
                    index_type,
                    display_ident(
                        &index_field.name,
                        force_quoted_ident,
                        quoted_ident_case_sensitive,
                        sql_dialect
                    ),
                    column_names_str
                );
                if !options.is_empty() {
                    let options_str = options.join(", ").to_string();
                    index_str.push(' ');
                    index_str.push_str(&options_str);
                }
                create_defs.push(index_str);
            }

            // Format is:
            //  (
            //      x,
            //      y
            //  )
            let create_defs_str = format!("{}\n", create_defs.join(",\n"));
            table_create_sql.push_str(&create_defs_str);
        }
        let table_engine = format!(") ENGINE={}", engine);
        table_create_sql.push_str(table_engine.as_str());

        if let Some(cluster_keys_str) = &table_info.meta.cluster_key {
            let cluster_type = table_info
                .options()
                .get(OPT_KEY_CLUSTER_TYPE)
                .cloned()
                .unwrap_or("".to_string());
            table_create_sql
                .push_str(format!(" CLUSTER BY {}{}", cluster_type, cluster_keys_str).as_str());
        }

        if !hide_options_in_show_create_table || engine == "ICEBERG" || engine == "DELTA" {
            table_create_sql.push_str({
                let mut opts = table_info.options().iter().collect::<Vec<_>>();
                opts.sort_by_key(|(k, _)| *k);
                opts.iter()
                    .filter(|(k, _)| !is_internal_opt_key(k))
                    .map(|(k, v)| format!(" {}='{}'", k.to_uppercase(), v))
                    .collect::<Vec<_>>()
                    .join("")
                    .as_str()
            });
        }

        if engine != "ICEBERG" && engine != "DELTA" {
            if let Some(sp) = &table_info.meta.storage_params {
                table_create_sql.push_str(format!(" '{}' ", sp).as_str());
            }
        }

        if !table_info.meta.comment.is_empty() {
            table_create_sql.push_str(
                format!(
                    " COMMENT = {}",
                    QuotedString(&table_info.meta.comment, '\'')
                )
                .as_str(),
            );
        }
        Ok(table_create_sql)
    }

    fn show_create_view_query(table: &dyn Table, database: &str) -> Result<String> {
        let name = table.name();
        let view_create_sql = if let Some(query) = table.options().get(QUERY) {
            Ok(format!(
                "CREATE VIEW `{}`.`{}` AS {}",
                database, name, query
            ))
        } else {
            Err(ErrorCode::Internal(
                "Logical error, View Table must have a SelectQuery inside.",
            ))
        }?;
        Ok(view_create_sql)
    }

    async fn show_create_stream_query(catalog: &dyn Catalog, table: &dyn Table) -> Result<String> {
        let stream_table = StreamTable::try_from_table(table)?;
        let source_database_name = stream_table.source_database_name(catalog).await?;
        let source_table_name = stream_table.source_table_name(catalog).await?;
        let mode = stream_table.mode();

        let mut create_sql = format!(
            "CREATE STREAM `{}` ON TABLE `{}`.`{}`",
            stream_table.name(),
            source_database_name,
            source_table_name
        );

        if matches!(mode, StreamMode::Standard) {
            create_sql.push_str(" APPEND_ONLY = false");
        }

        let comment = stream_table.get_table_info().meta.comment.clone();
        if !comment.is_empty() {
            create_sql.push_str(format!(" COMMENT = {}", QuotedString(comment, '\'')).as_str());
        }
        Ok(create_sql)
    }

    fn show_attach_table_query(table: &dyn Table, database: &str) -> String {
        // Note: Tables that attached before this PR #13403, could not show location properly
        let location_not_available = "N/A".to_string();
        let table_data_location = table
            .options()
            .get(OPT_KEY_TABLE_ATTACHED_DATA_URI)
            .unwrap_or(&location_not_available);

        let table_info = table.get_table_info();

        let mut include_cols = "".to_string();
        if table_info
            .meta
            .schema
            .metadata
            .contains_key(FUSE_OPT_KEY_ATTACH_COLUMN_IDS)
        {
            let cols = table_info
                .meta
                .schema
                .fields
                .iter()
                .map(|f| &f.name)
                .join(",");
            include_cols = format!("({cols}) ");
        }

        format!(
            "ATTACH TABLE {}`{}`.`{}` {}",
            include_cols,
            database,
            table.name(),
            table_data_location,
        )
    }
}
