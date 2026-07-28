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

use databend_common_ast::ast::CreateMaterializedViewStmt;
use databend_common_ast::ast::DropMaterializedViewStmt;
use databend_common_ast::ast::ShowCreateMaterializedViewStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowMaterializedViewsStmt;
use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_ast::ast::quote::QuotedString;
use databend_common_ast::visit::WalkMut;
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_meta_app::schema::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_AGGRESSIVE_RECLUSTER;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::is_fuse_engine;
use log::debug;

use crate::BindContext;
use crate::SelectBuilder;
use crate::ViewRewriter;
use crate::binder::Binder;
use crate::planner::binder::ddl::table::apply_fuse_storage_defaults;
use crate::planner::semantic::normalize_identifier;
use crate::plans::CreateMaterializedViewPlan;
use crate::plans::CreateTablePlan;
use crate::plans::DropMaterializedViewPlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::plans::ShowCreateMaterializedViewPlan;

fn is_supported_materialized_view_source(table: &dyn Table) -> bool {
    !table.is_temp()
        && !table.is_stream()
        && !table.is_read_only()
        && is_fuse_engine(table.engine())
}

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_materialized_view(
        &mut self,
        stmt: &CreateMaterializedViewStmt,
    ) -> Result<Plan> {
        let CreateMaterializedViewStmt {
            create_option,
            sync_creation,
            catalog,
            database,
            view,
            cluster_by,
            origin_query,
        } = stmt;

        let tenant = self.ctx.get_tenant();

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

        let original_query = origin_query.to_string();
        let mut qualified_query = origin_query.as_ref().clone();
        let mut rewriter = ViewRewriter {
            current_database: self.ctx.get_current_database(),
        };
        qualified_query.walk_mut(&mut rewriter)?;

        // TODO(materialized-view-query-validation): Add the Snowflake-compatible
        // materialized-view query restrictions in a dedicated change. This
        // create/drop phase intentionally adds no query-shape restrictions;
        // the single-source check below is only a metadata invariant needed to
        // record one source binding. In particular, deduplicating source IDs
        // here does not promise refresh support for same-source UNION/UNION ALL;
        // that capability must be implemented and tested explicitly later.
        //
        // TODO(materialized-view-source-fence): Derive the schema with two
        // independent bind passes, each using a fresh Binder and Metadata. The
        // first pass identifies the source, then reads its binding generation;
        // the second derives the final schema and plan and must resolve to the
        // same source. CREATE must CAS that generation when publishing the MV.
        // This fences source DDL that races with schema derivation.
        let as_query_plan = self.as_query_plan(&qualified_query).await?;
        let bind_context = as_query_plan.bind_context().unwrap();

        let (source_catalog_name, source_database_name, source_table_name, source_table) =
            if let Plan::Query { metadata, .. } = &as_query_plan {
                let metadata = metadata.read();
                let Some(first_source) = metadata.tables().first() else {
                    return Err(ErrorCode::SemanticError(
                        "Materialized view requires one persistent table source".to_string(),
                    ));
                };
                let source_table = first_source.table();
                if metadata.tables().iter().any(|entry| {
                    entry.catalog() != first_source.catalog()
                        || entry.table().get_id() != source_table.get_id()
                }) {
                    return Err(ErrorCode::SemanticError(
                        "Materialized view requires exactly one base table source".to_string(),
                    ));
                }

                (
                    first_source.catalog().to_string(),
                    first_source.database().to_string(),
                    first_source.name().to_string(),
                    source_table,
                )
            } else {
                unreachable!("AS query plan must be a query")
            };

        let valid_source = source_catalog_name.eq_ignore_ascii_case(CATALOG_DEFAULT)
            && is_supported_materialized_view_source(source_table.as_ref());
        if !valid_source {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "Materialized view source '{}.{}.{}' must be a persistent base table in the default catalog using FUSE engine",
                source_catalog_name, source_database_name, source_table_name
            )));
        }
        if !source_table.change_tracking_enabled() {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "Change tracking is not enabled on materialized view source '{}.{}.{}'; enable it before creating the materialized view",
                source_catalog_name, source_database_name, source_table_name
            )));
        }

        let source_table_id = source_table.get_id();
        let source_catalog = self.ctx.get_catalog(&source_catalog_name).await?;
        let expected_source_generation = source_catalog
            .get_mv_source_generation(&tenant, source_table_id)
            .await?;

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

        let query = format!("{}", qualified_query);

        let mut options = std::collections::BTreeMap::new();
        options.insert(
            OPT_KEY_DATABASE_ID.to_owned(),
            target_database_id.to_string(),
        );
        options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID.to_owned(),
            source_table_id.to_string(),
        );
        let mut cluster_key = None;
        if let Some(cluster_opt) = cluster_by {
            let keys = self
                .analyze_cluster_keys(cluster_opt, schema.clone(), None)
                .await?;
            if !keys.is_empty() {
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

        apply_fuse_storage_defaults(&mut options, storage_params.as_ref())?;

        let table_plan = CreateTablePlan {
            create_option,
            tenant,
            catalog: catalog_name,
            database: database_name,
            table: view_name,
            schema,
            engine: databend_common_ast::ast::Engine::MaterializedView,
            engine_options: Default::default(),
            storage_params,
            options,
            table_properties: None,
            table_partition: None,
            field_comments: vec![],
            field_stats_truncate_len: vec![],
            cluster_key,
            // The query is bound above to validate the definition and derive a
            // stable schema, but CREATE publishes an empty materialized view.
            // Data population belongs to the later maintenance phase.
            as_select: None,
            table_indexes: None,
            table_constraints: None,
            attached_columns: None,
        };
        Ok(Plan::CreateMaterializedView(Box::new(
            CreateMaterializedViewPlan {
                table_plan,
                query_plan: Box::new(as_query_plan),
                original_query,
                query,
                sync_creation: *sync_creation,
                expected_source_generation,
            },
        )))
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
        Ok(Plan::ShowCreateMaterializedView(Box::new(
            ShowCreateMaterializedViewPlan {
                catalog: catalog_name,
                database: database_name,
                view_name,
                schema,
            },
        )))
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
