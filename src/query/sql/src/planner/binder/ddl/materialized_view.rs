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

use databend_common_ast::Span;
use databend_common_ast::ast::CreateMaterializedViewStmt;
use databend_common_ast::ast::DropMaterializedViewStmt;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::RefreshMaterializedViewStmt;
use databend_common_ast::ast::ShowCreateMaterializedViewStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowMaterializedViewsStmt;
use databend_common_ast::ast::TableRef;
use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_ast::ast::quote::QuotedString;
use databend_common_ast::ast::quote::ident_needs_quote;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_meta_app::storage::StorageParams;
use databend_storages_common_table_meta::table::OPT_KEY_AGGRESSIVE_RECLUSTER;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_QUERY;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_QUERY_CATALOG;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_QUERY_DATABASE;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;
use log::debug;

use crate::BindContext;
use crate::NameResolutionContext;
use crate::SelectBuilder;
use crate::binder::Binder;
use crate::planner::semantic::normalize_identifier;
use crate::plans::CreateTablePlan;
use crate::plans::DropMaterializedViewPlan;
use crate::plans::Plan;
use crate::plans::RefreshMaterializedViewPlan;
use crate::plans::RewriteKind;
use crate::plans::ShowCreateTablePlan;

#[derive(VisitorMut)]
#[visitor(Query, TableRef(enter))]
struct TableRefQualifier {
    catalog: Identifier,
    database: Identifier,
    name_resolution_ctx: NameResolutionContext,
    cte_scope_stack: Vec<Vec<String>>,
}

impl TableRefQualifier {
    fn enter_query(&mut self, query: &mut Query) {
        let cte_names = query
            .with
            .as_ref()
            .map(|w| {
                w.ctes
                    .iter()
                    .map(|c| {
                        self.name_resolution_ctx
                            .normalize_identifier(&c.alias.name)
                            .name
                    })
                    .collect()
            })
            .unwrap_or_default();
        self.cte_scope_stack.push(cte_names);
    }

    fn exit_query(&mut self, _query: &mut Query) {
        self.cte_scope_stack.pop();
    }

    fn enter_table_ref(&mut self, table_ref: &mut TableRef) {
        if table_ref.database.is_none() {
            let normalized = self
                .name_resolution_ctx
                .normalize_identifier(&table_ref.table)
                .name;
            let is_cte = self
                .cte_scope_stack
                .iter()
                .any(|scope| scope.iter().any(|n| n == &normalized));
            if !is_cte {
                table_ref.database = Some(self.database.clone());
                if table_ref.catalog.is_none() {
                    table_ref.catalog = Some(self.catalog.clone());
                }
            }
        } else if table_ref.catalog.is_none() {
            table_ref.catalog = Some(self.catalog.clone());
        }
    }
}

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_materialized_view(
        &mut self,
        stmt: &CreateMaterializedViewStmt,
    ) -> Result<Plan> {
        let CreateMaterializedViewStmt {
            create_option,
            catalog,
            database,
            view,
            cluster_by,
            query,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let query_catalog_name = self.ctx.get_current_catalog();
        let query_database_name = self.ctx.get_current_database();
        let (catalog_name, database_name, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let target_catalog = self.ctx.get_catalog(&catalog_name).await?;
        if target_catalog.support_partition() {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "Catalog '{}' does not support MATERIALIZED VIEW",
                target_catalog.name()
            )));
        }
        let target_database = target_catalog.get_database(&tenant, &database_name).await?;
        let target_database_id = target_database.get_db_info().database_id.db_id;

        let as_query_plan = self.as_query_plan(query).await?;

        let bind_context = as_query_plan.bind_context().unwrap();
        let mut schema = bind_context.output_table_schema(self.metadata.clone())?;
        let mut fields = schema.fields().clone();
        for field in fields.iter_mut() {
            if field.data_type == TableDataType::Null {
                field.data_type = TableDataType::String.wrap_nullable();
            } else if !field.data_type().is_nullable_or_null() && !self.is_column_not_null() {
                field.data_type = field.data_type().clone().wrap_nullable();
            }
        }
        schema = TableSchemaRefExt::create(fields);
        Self::validate_create_table_schema(&schema)?;

        let mut qualified_query = query.as_ref().clone();
        let needs_quote = |name: &str| -> Option<char> {
            if ident_needs_quote(name) {
                return Some('`');
            }
            if !self.name_resolution_ctx.unquoted_ident_case_sensitive
                && self.name_resolution_ctx.quoted_ident_case_sensitive
                && name.chars().any(|c| c.is_uppercase())
            {
                return Some('`');
            }
            None
        };
        let mut qualifier = TableRefQualifier {
            catalog: Identifier::from_name_with_quoted(
                Span::default(),
                &query_catalog_name,
                needs_quote(&query_catalog_name),
            ),
            database: Identifier::from_name_with_quoted(
                Span::default(),
                &query_database_name,
                needs_quote(&query_database_name),
            ),
            name_resolution_ctx: self.name_resolution_ctx.clone(),
            cte_scope_stack: Vec::new(),
        };
        qualified_query.drive_mut(&mut qualifier);
        let query_text = format!("{}", qualified_query);

        let mut options = std::collections::BTreeMap::new();
        options.insert(
            OPT_KEY_DATABASE_ID.to_owned(),
            target_database_id.to_string(),
        );
        options.insert(OPT_KEY_MATERIALIZED_VIEW.to_string(), "true".to_string());
        options.insert(OPT_KEY_MATERIALIZED_VIEW_QUERY.to_string(), query_text);
        options.insert(
            OPT_KEY_MATERIALIZED_VIEW_QUERY_CATALOG.to_string(),
            query_catalog_name,
        );
        options.insert(
            OPT_KEY_MATERIALIZED_VIEW_QUERY_DATABASE.to_string(),
            query_database_name,
        );

        let mut cluster_key = None;
        if let Some(cluster_opt) = cluster_by {
            let keys = self
                .analyze_cluster_keys(cluster_opt, schema.clone(), None)
                .await?;
            if !keys.is_empty() {
                options.insert(
                    OPT_KEY_CLUSTER_TYPE.to_owned(),
                    cluster_opt.cluster_type.to_string().to_lowercase(),
                );
                options
                    .entry(OPT_KEY_AGGRESSIVE_RECLUSTER.to_owned())
                    .or_insert_with(|| "1".to_owned());
                cluster_key = Some(format!("({})", keys.join(", ")));
            }
        }

        let create_option = create_option.clone().into();

        let storage_params = self
            .resolve_database_default_storage_params(target_database.as_ref())
            .await?;

        // Apply Fuse storage defaults (same logic as bind_create_table)
        let config = GlobalConfig::instance();
        let is_blocking_fs = matches!(
            storage_params.as_ref().unwrap_or(&config.storage.params),
            StorageParams::Fs(_)
        );
        if !options.contains_key(OPT_KEY_STORAGE_FORMAT) {
            let default_storage_format = match config.query.common.default_storage_format.as_str() {
                "" | "auto" | "native" => "parquet",
                _ => config.query.common.default_storage_format.as_str(),
            };
            options.insert(
                OPT_KEY_STORAGE_FORMAT.to_owned(),
                default_storage_format.to_owned(),
            );
        }
        if !options.contains_key(OPT_KEY_TABLE_COMPRESSION) {
            let default_compression = match config.query.common.default_compression.as_str() {
                "" | "auto" => {
                    if is_blocking_fs {
                        "lz4"
                    } else {
                        "zstd"
                    }
                }
                _ => config.query.common.default_compression.as_str(),
            };
            options.insert(
                OPT_KEY_TABLE_COMPRESSION.to_owned(),
                default_compression.to_owned(),
            );
        }

        let plan = CreateTablePlan {
            create_option,
            tenant,
            catalog: catalog_name,
            database: database_name,
            table: view_name,
            schema,
            engine: databend_common_ast::ast::Engine::Fuse,
            engine_options: Default::default(),
            storage_params,
            options,
            table_properties: None,
            table_partition: None,
            field_comments: vec![],
            field_stats_truncate_len: vec![],
            cluster_key,
            as_select: Some(Box::new(as_query_plan)),
            table_indexes: None,
            table_constraints: None,
            attached_columns: None,
        };
        Ok(Plan::CreateTable(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_materialized_view(
        &mut self,
        stmt: &DropMaterializedViewStmt,
    ) -> Result<Plan> {
        let DropMaterializedViewStmt {
            if_exists,
            catalog,
            database,
            view,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let plan = DropMaterializedViewPlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
            view_name,
        };
        Ok(Plan::DropMaterializedView(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_refresh_materialized_view(
        &mut self,
        stmt: &RefreshMaterializedViewStmt,
    ) -> Result<Plan> {
        let RefreshMaterializedViewStmt {
            catalog,
            database,
            view,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let plan = RefreshMaterializedViewPlan {
            tenant,
            catalog,
            database,
            view_name,
        };
        Ok(Plan::RefreshMaterializedView(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_create_materialized_view(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &ShowCreateMaterializedViewStmt,
    ) -> Result<Plan> {
        let ShowCreateMaterializedViewStmt {
            catalog,
            database,
            view,
        } = stmt;

        let (catalog_name, database_name, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Table", DataType::String),
            DataField::new("Create Table", DataType::String),
        ]);
        Ok(Plan::ShowCreateTable(Box::new(ShowCreateTablePlan {
            catalog: catalog_name,
            database: database_name,
            table: view_name,
            schema,
            with_quoted_ident: false,
            require_materialized_view: true,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_materialized_views(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowMaterializedViewsStmt,
    ) -> Result<Plan> {
        let ShowMaterializedViewsStmt {
            catalog,
            database,
            limit,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => normalize_identifier(ident, &self.name_resolution_ctx).name,
        };

        let database_name = self.check_database_exist(catalog, database).await?;

        let mut select_builder = SelectBuilder::from(&format!(
            "{}.system.tables",
            QuotedIdent(catalog_name.to_lowercase(), '`')
        ));
        select_builder
            .with_column("name AS Name")
            .with_column("database AS Database")
            .with_column("engine AS Engine")
            .with_column("created_on AS \"Created On\"");

        select_builder.with_filter(format!("database = {}", QuotedString(&database_name, '\'')));
        select_builder.with_filter(format!("catalog = {}", QuotedString(&catalog_name, '\'')));
        select_builder.with_filter("table_type = 'MATERIALIZED VIEW'".to_string());

        select_builder
            .with_order_by("database")
            .with_order_by("name");

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE {}", QuotedString(pattern, '\'')));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show materialized views rewrite to: {:?}", query);
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowTables(catalog_name, database_name),
        )
        .await
    }
}
