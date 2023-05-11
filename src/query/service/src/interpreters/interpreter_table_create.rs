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

use std::collections::HashSet;
use std::sync::Arc;

use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::schema::TableStatistics;
use common_meta_types::MatchSeq;
use common_sql::binder::INTERNAL_COLUMN_FACTORY;
use common_sql::field_default_value;
use common_sql::plans::CreateTablePlan;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use common_storages_fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use common_storages_fuse::FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD;
use common_storages_fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use common_storages_fuse::FUSE_OPT_KEY_ROW_PER_PAGE;
use common_users::UserApiProvider;
use once_cell::sync::Lazy;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use storages_common_table_meta::table::OPT_KEY_COMMENT;
use storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use storages_common_table_meta::table::OPT_KEY_ENGINE;
use storages_common_table_meta::table::OPT_KEY_EXTERNAL_LOCATION;
use storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use tracing::error;

use crate::interpreters::InsertInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::insert::Insert;
use crate::sql::plans::insert::InsertInputSource;
use crate::sql::plans::Plan;
use crate::storages::StorageDescription;

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
        "CreateTableInterpreterV2"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.plan.tenant.clone();
        let quota_api = UserApiProvider::instance().get_tenant_quota_api_client(&tenant)?;
        let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
        let engine = self.plan.engine;
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str())?;
        let tables = catalog
            .list_tables(&self.plan.tenant, &self.plan.database)
            .await?;
        if quota.max_tables_per_database != 0
            && tables.len() >= quota.max_tables_per_database as usize
        {
            return Err(ErrorCode::TenantQuotaExceeded(format!(
                "Max tables per database quota exceeded: {}",
                quota.max_tables_per_database
            )));
        };
        let name_not_duplicate = tables
            .iter()
            .all(|table| table.name() != self.plan.table.as_str());

        let engine_desc: Option<StorageDescription> = catalog
            .get_table_engines()
            .iter()
            .find(|desc| {
                desc.engine_name.to_string().to_lowercase() == engine.to_string().to_lowercase()
            })
            .cloned();

        match engine_desc {
            Some(engine) => {
                if self.plan.cluster_key.is_some() && !engine.support_cluster_key {
                    return Err(ErrorCode::UnsupportedEngineParams(format!(
                        "Unsupported cluster key for engine: {}",
                        engine.engine_name
                    )));
                }
            }
            None => {
                if name_not_duplicate {
                    return Err(ErrorCode::UnknownTableEngine(format!(
                        "Unknown table engine {}",
                        engine
                    )));
                }
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
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(&self.plan.catalog)?;

        // TODO: maybe the table creation and insertion should be a transaction, but it may require create_table support 2pc.
        catalog.create_table(self.build_request(None)?).await?;
        let table = catalog
            .get_table(tenant.as_str(), &self.plan.database, &self.plan.table)
            .await?;

        // If the table creation query contains column definitions, like 'CREATE TABLE t1(a int) AS SELECT * from t2',
        // we use the definitions to create the table schema. It may happen that the "AS SELECT" query's schema doesn't
        // match the table's schema. For example,
        //
        //   mysql> create table t2(a int, b int);
        //   mysql> create table t1(x string, y string) as select * from t2;
        //
        // For the situation above, we implicitly cast the data type when inserting data.
        // The casting and schema checking is in interpreter_insert.rs, function check_schema_cast.
        let insert_plan = Insert {
            catalog: self.plan.catalog.clone(),
            database: self.plan.database.clone(),
            table: self.plan.table.clone(),
            table_id: table.get_id(),
            schema: self.plan.schema.clone(),
            overwrite: false,
            source: InsertInputSource::SelectPlan(select_plan),
        };

        InsertInterpreter::try_create(self.ctx.clone(), insert_plan)?
            .execute2()
            .await
    }

    #[async_backtrace::framed]
    async fn create_table(&self) -> Result<PipelineBuildResult> {
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str())?;
        let mut stat = None;
        if !GlobalConfig::instance().query.management_mode {
            if let Some(snapshot_loc) = self.plan.options.get(OPT_KEY_SNAPSHOT_LOCATION) {
                let operator = self.ctx.get_data_operator()?.operator();
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
                });
            }
        }
        catalog.create_table(self.build_request(stat)?).await?;

        Ok(PipelineBuildResult::create())
    }

    /// Build CreateTableReq from CreateTablePlanV2.
    ///
    /// - Rebuild `DataSchema` with default exprs.
    /// - Update cluster key of table meta.
    fn build_request(&self, statistics: Option<TableStatistics>) -> Result<CreateTableReq> {
        let mut fields = Vec::with_capacity(self.plan.schema.num_fields());
        for (idx, field) in self.plan.schema.fields().clone().into_iter().enumerate() {
            let field = if let Some(Some(default_expr)) = &self.plan.field_default_exprs.get(idx) {
                let field = field.with_default_expr(Some(default_expr.clone()));
                let _ = field_default_value(self.ctx.clone(), &field)?;
                field
            } else {
                field
            };

            if INTERNAL_COLUMN_FACTORY.exist(field.name()) {
                return Err(ErrorCode::TableWithInternalColumnName(format!(
                    "Cannot create table has column with the same name as internal column: {}",
                    field.name()
                )));
            }

            fields.push(field)
        }
        let schema = TableSchemaRefExt::create(fields);

        let mut table_meta = TableMeta {
            schema,
            engine: self.plan.engine.to_string(),
            storage_params: self.plan.storage_params.clone(),
            part_prefix: self.plan.part_prefix.clone(),
            options: self.plan.options.clone(),
            default_cluster_key: None,
            field_comments: self.plan.field_comments.clone(),
            drop_on: None,
            statistics: if let Some(stat) = statistics {
                stat
            } else {
                Default::default()
            },
            ..Default::default()
        };

        for table_option in table_meta.options.iter() {
            let key = table_option.0.to_lowercase();
            if !is_valid_create_opt(&key) {
                error!("invalid opt for fuse table in create table statement");
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "table option {key} is invalid for create table statement",
                )));
            }
        }

        if let Some(cluster_key) = &self.plan.cluster_key {
            table_meta = table_meta.push_cluster_key(cluster_key.clone());
        }

        let req = CreateTableReq {
            if_not_exists: self.plan.if_not_exists,
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.to_string(),
                db_name: self.plan.database.to_string(),
                table_name: self.plan.table.to_string(),
            },
            table_meta,
        };

        Ok(req)
    }
}

/// Table option keys that can occur in 'create table statement'.
pub static CREATE_TABLE_OPTIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    let mut r = HashSet::new();
    r.insert(FUSE_OPT_KEY_ROW_PER_PAGE);
    r.insert(FUSE_OPT_KEY_BLOCK_PER_SEGMENT);
    r.insert(FUSE_OPT_KEY_ROW_PER_BLOCK);
    r.insert(FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD);
    r.insert(FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD);

    r.insert(OPT_KEY_SNAPSHOT_LOCATION);
    r.insert(OPT_KEY_LEGACY_SNAPSHOT_LOC);
    r.insert(OPT_KEY_TABLE_COMPRESSION);
    r.insert(OPT_KEY_STORAGE_FORMAT);
    r.insert(OPT_KEY_DATABASE_ID);

    r.insert(OPT_KEY_COMMENT);
    r.insert(OPT_KEY_EXTERNAL_LOCATION);
    r.insert(OPT_KEY_ENGINE);

    r.insert("transient");
    r
});

pub fn is_valid_create_opt<S: AsRef<str>>(opt_key: S) -> bool {
    CREATE_TABLE_OPTIONS.contains(opt_key.as_ref().to_lowercase().as_str())
}
