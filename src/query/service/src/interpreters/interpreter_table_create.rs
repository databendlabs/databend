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

use std::str::FromStr;
use std::sync::Arc;

use chrono::Utc;
use databend_common_ast::ast::Engine;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::is_internal_column;
use databend_common_license::license::Feature;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::TablePartition;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::tenant::Tenant;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::DefaultExprBinder;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_storages_fuse::FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE;
use databend_common_storages_fuse::FUSE_OPT_KEY_ENABLE_AUTO_VACUUM;
use databend_common_storages_fuse::FuseSegmentFormat;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_enterprise_attach_table::get_attach_table_handler;
use databend_meta_types::MatchSeq;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::OPT_KEY_COMMENT;
use databend_storages_common_table_meta::table::OPT_KEY_ENABLE_COPY_DEDUP_FULL_PATH;
use databend_storages_common_table_meta::table::OPT_KEY_SEGMENT_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_PREFIX;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use log::error;
use log::info;

use crate::interpreters::InsertInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::common::table_option_validation::is_valid_approx_distinct_columns;
use crate::interpreters::common::table_option_validation::is_valid_block_per_segment;
use crate::interpreters::common::table_option_validation::is_valid_bloom_index_columns;
use crate::interpreters::common::table_option_validation::is_valid_change_tracking;
use crate::interpreters::common::table_option_validation::is_valid_create_opt;
use crate::interpreters::common::table_option_validation::is_valid_data_retention_period;
use crate::interpreters::common::table_option_validation::is_valid_fuse_parquet_dictionary_opt;
use crate::interpreters::common::table_option_validation::is_valid_option_of_type;
use crate::interpreters::common::table_option_validation::is_valid_random_seed;
use crate::interpreters::common::table_option_validation::is_valid_row_per_block;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::PipelineBuildResult;
use crate::servers::http::v1::ClientSessionManager;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::Insert;
use crate::sql::plans::InsertInputSource;
use crate::sql::plans::Plan;
use crate::storages::StorageDescription;

#[derive(Clone, Debug)]
pub struct CreateTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateTablePlan,
}

impl CreateTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTablePlan) -> Result<Self> {
        Ok(CreateTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTableInterpreter {
    fn name(&self) -> &str {
        "CreateTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = &self.plan.tenant;

        let has_computed_column = self
            .plan
            .schema
            .fields()
            .iter()
            .any(|f| f.computed_expr().is_some());
        if has_computed_column {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.ctx.get_license_key(), ComputedColumn)?;
        }

        let quota_api = UserApiProvider::instance().tenant_quota_api(tenant);
        let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
        let engine = self.plan.engine;
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        if quota.max_tables_per_database > 0 {
            // Note:
            // max_tables_per_database is a config quota. Default is 0.
            // If a database has lot of tables, list_tables will be slow.
            // So We check get it when max_tables_per_database != 0
            let tables = catalog
                .list_tables(&self.plan.tenant, &self.plan.database)
                .await?;
            if tables.len() >= quota.max_tables_per_database as usize {
                return Err(ErrorCode::TenantQuotaExceeded(format!(
                    "Max tables per database quota exceeded: {}",
                    quota.max_tables_per_database
                )));
            }
        }

        let engine_desc: Option<StorageDescription> = catalog
            .get_table_engines()
            .iter()
            .find(|desc| {
                desc.engine_name.to_string().to_lowercase() == engine.to_string().to_lowercase()
            })
            .cloned();

        if let Some(engine) = engine_desc {
            if self.plan.cluster_key.is_some() && !engine.support_cluster_key {
                return Err(ErrorCode::UnsupportedEngineParams(format!(
                    "Unsupported cluster key for engine: {}",
                    engine.engine_name
                )));
            }
        }

        match &self.plan.as_select {
            Some(select_plan_node) => self.create_table_as_select(select_plan_node.clone()).await,
            None => self.create_table().await,
        }
    }
}

impl CreateTableInterpreter {
    #[async_backtrace::framed]
    async fn create_table_as_select(&self, select_plan: Box<Plan>) -> Result<PipelineBuildResult> {
        assert!(
            !self.plan.options.contains_key(OPT_KEY_STORAGE_PREFIX),
            "There should be no ATTACH TABLE AS SELECT plan"
        );

        let tenant = self.ctx.get_tenant();

        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        let mut req = self.build_request(None)?;

        // create a dropped table first.
        req.as_dropped = true;
        req.table_meta.drop_on = Some(Utc::now());
        let table_meta = req.table_meta.clone();
        let reply = catalog.create_table(req.clone()).await?;
        if !reply.new_table && self.plan.create_option != CreateOption::CreateOrReplace {
            return Ok(PipelineBuildResult::create());
        }
        if let Some(prefix) = req.table_meta.options.get(OPT_KEY_TEMP_PREFIX).cloned() {
            self.register_temp_table(prefix).await?;
        }

        let table_id = reply.table_id;
        let prev_table_id = reply.prev_table_id;
        let orphan_table_name = reply.orphan_table_name.clone();
        let table_id_seq = reply
            .table_id_seq
            .expect("internal error: table_id_seq must have been set. CTAS(replace) of table");
        let db_id = reply.db_id;

        if !req.table_meta.options.contains_key(OPT_KEY_TEMP_PREFIX) {
            self.process_ownership(&tenant, reply).await?;
        }

        // If the table creation query contains column definitions, like 'CREATE TABLE t1(a int) AS SELECT * from t2',
        // we use the definitions to create the table schema. It may happen that the "AS SELECT" query's schema doesn't
        // match the table's schema. For example,
        //
        //   mysql> create table t2(a int, b int);
        //   mysql> create table t1(x string, y string) as select * from t2;
        //
        // For the situation above, we implicitly cast the data type when inserting data.
        // The casting and schema checking is in interpreter_insert.rs, function check_schema_cast.

        let table_info = TableInfo::new(
            &self.plan.database,
            &self.plan.table,
            TableIdent::new(table_id, table_id_seq),
            table_meta,
        );

        let insert_plan = Insert {
            catalog: self.plan.catalog.clone(),
            database: self.plan.database.clone(),
            table: self.plan.table.clone(),
            branch: None,
            schema: self.plan.schema.clone(),
            overwrite: false,
            source: InsertInputSource::SelectPlan(select_plan),
            table_info: Some(table_info),
        };

        let mut pipeline = InsertInterpreter::try_create(self.ctx.clone(), insert_plan)?
            .execute2()
            .await?;

        let db_name = self.plan.database.clone();
        let table_name = self.plan.table.clone();

        let query_ctx = self.ctx.clone();
        pipeline
            .main_pipeline
            .set_on_finished(always_callback(move |_: &ExecutionInfo| {
                hook_clear_m_cte_temp_table(&query_ctx)?;
                hook_vacuum_temp_files(&query_ctx)?;
                hook_disk_temp_dir(&query_ctx)?;
                Ok(())
            }));
        // Add a callback to restore table visibility upon successful insert pipeline completion.
        // As there might be previous on_finish callbacks(e.g. refresh/compact/re-cluster hooks) which
        // depend on the table being visible, this callback is added at the beginning of the on_finish
        // callback list.
        //
        // If the un-drop fails, data inserted and the table will be invisible, and available for vacuum.

        let ctx = self.ctx.clone();
        pipeline
            .main_pipeline
            .lift_on_finished(move |info: &ExecutionInfo| {
                info!("{:?}", ctx.session_state()?.temp_tbl_mgr);
                let qualified_table_name = format!("{}.{}", db_name, table_name);

                if info.res.is_ok() {
                    info!(
                        "create_table_as_select {} success, commit table meta data by table id {}",
                        qualified_table_name, table_id
                    );
                    let fut = async move {
                        let req = CommitTableMetaReq {
                            name_ident: TableNameIdent {
                                tenant,
                                db_name,
                                table_name,
                            },
                            db_id,
                            table_id,
                            prev_table_id,
                            orphan_table_name,
                        };
                        catalog.commit_table_meta(req).await
                    };

                    GlobalIORuntime::instance().block_on(fut).map_err(|e| {
                        info!("create {} as select failed. {:?}", qualified_table_name, e);
                        e
                    })?;
                    info!("{:?}", ctx.session_state()?.temp_tbl_mgr);
                }

                Ok(())
            });

        Ok(pipeline)
    }

    // revoke ownership handling is now integrated into the create_table transaction
    async fn process_ownership(&self, tenant: &Tenant, reply: CreateTableReply) -> Result<()> {
        // grant the ownership of the table to the current role.
        let current_role = self.ctx.get_current_role();
        let role_api = UserApiProvider::instance().role_api(tenant);
        if let Some(current_role) = current_role {
            role_api
                .grant_ownership(
                    &OwnershipObject::Table {
                        catalog_name: self.plan.catalog.clone(),
                        db_id: reply.db_id,
                        table_id: reply.table_id,
                    },
                    &current_role.name,
                )
                .await?;
            RoleCacheManager::instance().invalidate_cache(tenant);
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn create_table(&self) -> Result<PipelineBuildResult> {
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        let mut stat = None;
        if !GlobalConfig::instance().query.common.management_mode {
            if let Some(snapshot_loc) = self.plan.options.get(OPT_KEY_SNAPSHOT_LOCATION) {
                // using application level data operator is a temp workaround
                // please see discussions https://github.com/datafuselabs/databend/pull/10424
                let operator = self.ctx.get_application_level_data_operator()?.operator();
                let reader = MetaReaders::table_snapshot_reader(operator);

                let params = LoadParams {
                    location: snapshot_loc.clone(),
                    len_hint: None,
                    ver: TableSnapshot::VERSION,
                    put_cache: true,
                };

                let snapshot = reader.read(&params).await?;
                stat = Some(TableStatistics {
                    number_of_rows: snapshot.summary.row_count,
                    data_bytes: snapshot.summary.uncompressed_byte_size,
                    compressed_data_bytes: snapshot.summary.compressed_byte_size,
                    index_data_bytes: snapshot.summary.index_size,
                    bloom_index_size: snapshot.summary.bloom_index_size,
                    ngram_index_size: snapshot.summary.ngram_index_size,
                    inverted_index_size: snapshot.summary.inverted_index_size,
                    vector_index_size: snapshot.summary.vector_index_size,
                    virtual_column_size: snapshot.summary.virtual_column_size,
                    number_of_segments: Some(snapshot.segments.len() as u64),
                    number_of_blocks: Some(snapshot.summary.block_count),
                });
            }
        }
        let req = if let Some(storage_prefix) = self.plan.options.get(OPT_KEY_STORAGE_PREFIX) {
            self.build_attach_request(storage_prefix).await
        } else {
            self.build_request(stat)
        }?;

        if !catalog.support_partition()
            && (req.table_properties.is_some() || req.table_partition.is_some())
        {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "Current Catalog Type is {:?}, only Iceberg Catalog supports CREATE TABLE with PARTITION BY or PROPERTIES",
                catalog.info().catalog_type()
            )));
        }

        let reply = catalog.create_table(req.clone()).await?;
        if let Some(prefix) = req.table_meta.options.get(OPT_KEY_TEMP_PREFIX).cloned() {
            self.register_temp_table(prefix).await?;
        }

        // iceberg table do not need to generate ownership.
        if !req.table_meta.options.contains_key(OPT_KEY_TEMP_PREFIX) && !catalog.is_external() {
            let tenant = self.ctx.get_tenant();
            self.process_ownership(&tenant, reply).await?;
        }

        Ok(PipelineBuildResult::create())
    }

    /// Build CreateTableReq from CreateTablePlanV2.
    ///
    /// - Rebuild `DataSchema` with default exprs.
    /// - Update cluster key of table meta.
    fn build_request(&self, statistics: Option<TableStatistics>) -> Result<CreateTableReq> {
        let fields = self.plan.schema.fields().clone();
        let mut default_expr_binder = DefaultExprBinder::try_new(self.ctx.clone())?;
        for field in fields.iter() {
            if field.default_expr().is_some() {
                let _ = default_expr_binder.get_scalar(field)?;
            }
            is_valid_column(field.name())?;
        }
        let field_comments = if self.plan.field_comments.is_empty() {
            vec!["".to_string(); fields.len()]
        } else {
            self.plan.field_comments.clone()
        };
        let schema = TableSchemaRefExt::create(fields);
        let mut options = self.plan.options.clone();

        if self.plan.engine == Engine::Fuse {
            let settings = self.ctx.get_settings();
            // change default to 1 when all query server is ready to processing it.
            if settings.get_copy_dedup_full_path_by_default()? {
                options.insert(
                    OPT_KEY_ENABLE_COPY_DEDUP_FULL_PATH.to_string(),
                    "true".to_string(),
                );
            };

            if !options.contains_key(FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE) {
                options.insert(
                    FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE.to_string(),
                    "1".to_string(),
                );
            }
        }
        if let Some(storage_format) = options.get(OPT_KEY_STORAGE_FORMAT) {
            FuseStorageFormat::from_str(storage_format)?;
        }
        if let Some(segment_format) = options.get(OPT_KEY_SEGMENT_FORMAT) {
            FuseSegmentFormat::from_str(segment_format)?;
        }
        let comment = options.remove(OPT_KEY_COMMENT);
        let indexes = self.plan.table_indexes.clone().unwrap_or_default();
        let constraints = self.plan.table_constraints.clone().unwrap_or_default();

        let mut table_meta = TableMeta {
            schema: schema.clone(),
            engine: self.plan.engine.to_string(),
            storage_params: self.plan.storage_params.clone(),
            options,
            engine_options: self.plan.engine_options.clone(),
            field_comments,
            drop_on: None,
            statistics: statistics.unwrap_or_default(),
            comment: comment.unwrap_or_default(),
            indexes,
            constraints,
            ..Default::default()
        };

        is_valid_block_per_segment(&table_meta.options)?;
        is_valid_row_per_block(&table_meta.options)?;
        // check bloom_index_columns.
        is_valid_bloom_index_columns(&table_meta.options, schema.clone())?;
        is_valid_approx_distinct_columns(&table_meta.options, schema)?;
        is_valid_change_tracking(&table_meta.options)?;
        // check random seed
        is_valid_random_seed(&table_meta.options)?;
        // check table level data_retention_period_in_hours
        is_valid_data_retention_period(&table_meta.options)?;
        // check enable_parquet_encoding
        is_valid_fuse_parquet_dictionary_opt(&table_meta.options)?;

        // Same as settings of FUSE_OPT_KEY_ENABLE_AUTO_VACUUM, expect value type is unsigned integer
        is_valid_option_of_type::<u32>(&table_meta.options, FUSE_OPT_KEY_ENABLE_AUTO_VACUUM)?;
        // check enable auto analyze.
        is_valid_option_of_type::<u32>(&table_meta.options, FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE)?;

        for table_option in table_meta.options.iter() {
            let key = table_option.0.to_lowercase();
            if !is_valid_create_opt(&key, &self.plan.engine) {
                let msg = format!(
                    "table option {key} is invalid for create table statement with engine {}",
                    self.plan.engine
                );
                error!("{msg}");
                return Err(ErrorCode::TableOptionInvalid(msg));
            }
        }

        if let Some(cluster_key) = &self.plan.cluster_key {
            table_meta.cluster_key_seq += 1;
            table_meta.cluster_key_v2 = Some((table_meta.cluster_key_seq, cluster_key.clone()));
        }

        let req = CreateTableReq {
            create_option: self.plan.create_option,
            catalog_name: if self.plan.create_option.is_overriding() {
                Some(self.plan.catalog.to_string())
            } else {
                None
            },
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.clone(),
                db_name: self.plan.database.to_string(),
                table_name: self.plan.table.to_string(),
            },
            table_meta,
            as_dropped: false,
            table_properties: self.plan.table_properties.clone(),
            table_partition: self.plan.table_partition.as_ref().map(|table_partition| {
                TablePartition::Identity {
                    columns: table_partition.clone(),
                }
            }),
        };

        Ok(req)
    }

    async fn build_attach_request(&self, storage_prefix: &str) -> Result<CreateTableReq> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::AttacheTable)?;

        let handler = get_attach_table_handler();
        handler
            .build_attach_table_request(storage_prefix, &self.plan)
            .await
    }

    async fn register_temp_table(&self, prefix: String) -> Result<()> {
        let session = self.ctx.get_current_session();
        if let Some(id) = session.get_client_session_id() {
            let client_session_manager = ClientSessionManager::instance();
            client_session_manager.add_temp_tbl_mgr(prefix, session.temp_tbl_mgr().clone());
            client_session_manager
                .refresh_session_handle(
                    self.ctx.get_tenant(),
                    self.ctx.get_current_user()?.name,
                    &id,
                )
                .await?;
        }
        Ok(())
    }
}

pub fn is_valid_column(name: &str) -> Result<()> {
    if is_internal_column(name) {
        return Err(ErrorCode::TableWithInternalColumnName(format!(
            "Cannot create table has column with the same name as internal column: {}",
            name
        )));
    }
    Ok(())
}
