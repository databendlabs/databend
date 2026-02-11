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
use databend_common_ast::ast::TypeName;
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
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use databend_storages_common_table_meta::table::OPT_KEY_TARGET_LAG;

use crate::BindContext;
use crate::Binder;
use crate::binder::ddl::table::AnalyzeCreateTableResult;
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

            options.insert(OPT_KEY_AS_QUERY.to_owned(), format!("{as_query}"));
            options.insert(OPT_KEY_TARGET_LAG.to_owned(), format!("{target_lag}"));

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
                        "" | "auto" => {
                            if is_blocking_fs {
                                "native"
                            } else {
                                "parquet"
                            }
                        }
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

        // todo(geometry): remove this when geometry stable.
        if let Some(CreateTableSource::Columns {
            columns,
            opt_table_indexes,
            opt_column_constraints,
            opt_table_constraints,
        }) = &source
        {
            if columns
                .iter()
                .any(|col| matches!(col.data_type, TypeName::Geometry))
                && !self.ctx.get_settings().get_enable_geo_create_table()?
            {
                return Err(ErrorCode::GeometryError(
                    "Create table using the geometry type is an experimental feature. \
                    You can `set enable_geo_create_table=1` to use this feature. \
                    We do not guarantee its compatibility until we doc this feature.",
                ));
            }
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
                .analyze_cluster_keys(cluster_opt, schema.clone())
                .await?;
            if !keys.is_empty() {
                options.insert(
                    OPT_KEY_CLUSTER_TYPE.to_owned(),
                    format!("{}", cluster_opt.cluster_type).to_lowercase(),
                );
                cluster_key = Some(format!("({})", keys.join(", ")));
            }
        }

        let plan = CreateDynamicTablePlan {
            create_option: create_option.clone().into(),
            tenant: self.ctx.get_tenant(),
            catalog: catalog_name.clone(),
            database: database.clone(),
            table,
            schema: schema.clone(),
            options,
            field_comments,
            cluster_key,
            as_query: as_query.to_string(),
            target_lag: target_lag.clone(),
            warehouse_opts: warehouse_opts.clone(),
            refresh_mode: refresh_mode.clone(),
            initialize: initialize.clone(),
        };
        Ok(Plan::CreateDynamicTable(Box::new(plan)))
    }
}
