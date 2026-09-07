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

use std::collections::BTreeMap;

use databend_common_ast::ast::CreateDynamicTableStmt;
use databend_common_ast::ast::CreateTableSource;
use databend_common_ast::ast::Engine;
use databend_common_ast::ast::Query;
use databend_common_ast::visit::WalkMut;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_schema_type;
use databend_common_meta_app::storage::StorageParams;
use databend_storages_common_table_meta::table::OPT_KEY_AS_QUERY;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_INITIALIZE;
use databend_storages_common_table_meta::table::OPT_KEY_INITIALIZED;
use databend_storages_common_table_meta::table::OPT_KEY_REFRESH_MODE;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_ENDPOINTS;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use databend_storages_common_table_meta::table::OPT_KEY_TARGET_LAG;
use databend_storages_common_table_meta::table::is_fuse_engine;

use crate::BindContext;
use crate::Binder;
use crate::binder::ddl::table::AnalyzeCreateTableResult;
use crate::planner::semantic::ViewRewriter;
use crate::plans::CreateDynamicTablePlan;
use crate::plans::Plan;

impl Binder {
    pub(in crate::planner::binder) async fn bind_create_dynamic_table(
        &mut self,
        stmt: &CreateDynamicTableStmt,
    ) -> Result<Plan> {
        let CreateDynamicTableStmt {
            create_option,
            transient,
            catalog,
            database,
            table,
            source,
            cluster_by,
            target_lag,
            refresh_mode,
            warehouse_opts,
            initialize,
            table_options,
            as_query,
        } = stmt;

        let (catalog_name, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let mut options: BTreeMap<String, String> = BTreeMap::new();
        {
            // If table is TRANSIENT, set a flag in table option
            if *transient {
                options.insert("TRANSIENT".to_owned(), "T".to_owned());
            }

            options.insert(OPT_KEY_INITIALIZED.to_owned(), "false".to_string());
            options.insert(OPT_KEY_SOURCE_ENDPOINTS.to_owned(), "[]".to_string());
            options.insert(OPT_KEY_TARGET_LAG.to_owned(), format!("{target_lag}"));
            options.insert(OPT_KEY_REFRESH_MODE.to_owned(), format!("{refresh_mode}"));
            options.insert(OPT_KEY_INITIALIZE.to_owned(), format!("{initialize}"));

            let catalog = self.ctx.get_catalog(&catalog_name).await?;
            let db = catalog
                .get_database(&self.ctx.get_tenant(), &database)
                .await?;
            let db_id = db.get_db_info().database_id.db_id;
            options.insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());

            for table_option in table_options.iter() {
                self.insert_table_option_with_validation(
                    &mut options,
                    table_option.0.to_lowercase(),
                    table_option.1.to_string(),
                )?;
            }

            let config = GlobalConfig::instance();
            let is_blocking_fs = matches!(&config.storage.params, StorageParams::Fs(_));
            // we should persist the storage format and compression type instead of using the default value
            if !options.contains_key(OPT_KEY_STORAGE_FORMAT) {
                let default_storage_format =
                    match config.query.common.default_storage_format.as_str() {
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
        }

        if let Some(CreateTableSource::Columns {
            opt_table_indexes,
            opt_column_constraints,
            opt_table_constraints,
            ..
        }) = &source
        {
            if opt_table_indexes.is_some() {
                return Err(ErrorCode::SemanticError(
                    "dynamic table don't support indexes".to_string(),
                ));
            }
            if opt_column_constraints.is_some() || opt_table_constraints.is_some() {
                return Err(ErrorCode::SemanticError(
                    "dynamic table don't support constraints".to_string(),
                ));
            }
        }

        let mut init_bind_context = BindContext::new();
        let (_, bind_context) = self.bind_query(&mut init_bind_context, as_query)?;
        for source_entry in self.metadata.read().tables() {
            let source_table = source_entry.table();
            if source_entry.catalog() != catalog_name
                || source_table.is_temp()
                || source_table.is_stream()
                || source_table.is_read_only()
                || !is_fuse_engine(source_table.engine())
            {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "Dynamic Table sources must be persistent writable FUSE tables in catalog '{}', but '{}.{}.{}' uses engine {}",
                    catalog_name,
                    source_entry.catalog(),
                    source_entry.database(),
                    source_entry.name(),
                    source_table.engine(),
                )));
            }
        }
        let query_fields = bind_context
            .columns
            .iter()
            .map(|column_binding| {
                Ok(TableField::new(
                    &column_binding.column_name,
                    infer_schema_type(&column_binding.data_type)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let (schema, field_comments) = match source {
            Some(source) => {
                let AnalyzeCreateTableResult {
                    schema: source_schema,
                    field_comments: source_comments,
                    ..
                } = self.analyze_create_table_schema(&table, source).await?;
                if source_schema.fields().len() != query_fields.len() {
                    return Err(ErrorCode::BadArguments("Number of columns does not match"));
                }
                Self::validate_create_table_schema(&source_schema)?;
                (source_schema, source_comments)
            }
            None => {
                let schema = TableSchemaRefExt::create(query_fields);
                Self::validate_create_table_schema(&schema)?;
                (schema, vec![])
            }
        };

        let mut cluster_key = None;
        if let Some(cluster_opt) = cluster_by {
            let keys = self
                .analyze_cluster_keys(cluster_opt, schema.clone(), None, true)
                .await?;
            if !keys.is_empty() {
                options.insert(
                    OPT_KEY_CLUSTER_TYPE.to_owned(),
                    cluster_opt.cluster_type.to_string().to_lowercase(),
                );
                cluster_key = Some(format!("({})", keys.join(", ")));
            }
        }

        if !matches!(
            refresh_mode,
            databend_common_ast::ast::RefreshMode::Full
                | databend_common_ast::ast::RefreshMode::Auto
        ) {
            return Err(ErrorCode::Unimplemented(
                "Dynamic Table currently supports only FULL refresh mode",
            ));
        }
        if !matches!(target_lag, databend_common_ast::ast::TargetLag::Manual) {
            return Err(ErrorCode::Unimplemented(
                "Dynamic Table scheduling is not implemented; omit TARGET_LAG",
            ));
        }
        // The parser accepts WAREHOUSE for the eventual scheduled-refresh design. Refresh runs in
        // the caller's session today, so honouring it is impossible; reject it rather than
        // silently ignoring a warehouse the user explicitly asked for.
        if warehouse_opts.warehouse.is_some() {
            return Err(ErrorCode::Unimplemented(
                "Dynamic Table WAREHOUSE is not implemented; refresh runs in the current session",
            ));
        }

        let mut canonical_query: Query = as_query.as_ref().clone();
        canonical_query.walk_mut(&mut ViewRewriter {
            current_database: database.clone(),
        })?;
        options.insert(OPT_KEY_AS_QUERY.to_owned(), canonical_query.to_string());

        // Dynamic Table initialization is performed by its dedicated interpreter so that the
        // first successful refresh also publishes the source endpoint checkpoint.
        let as_select = None;
        let table_plan = crate::plans::CreateTablePlan {
            create_option: create_option.clone().into(),
            tenant: self.ctx.get_tenant(),
            catalog: catalog_name.clone(),
            database: database.clone(),
            table,
            schema: schema.clone(),
            engine: Engine::DynamicTable,
            engine_options: BTreeMap::new(),
            storage_params: None,
            options,
            table_properties: None,
            table_partition: None,
            field_comments,
            field_stats_truncate_len: vec![],
            cluster_key,
            as_select,
            table_indexes: None,
            table_constraints: None,
            attached_columns: None,
        };
        let plan = CreateDynamicTablePlan {
            table_plan,
            as_query: canonical_query.to_string(),
            target_lag: target_lag.clone(),
            warehouse_opts: warehouse_opts.clone(),
            refresh_mode: refresh_mode.clone(),
            initialize: initialize.clone(),
        };
        Ok(Plan::CreateDynamicTable(Box::new(plan)))
    }
}
