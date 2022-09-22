// Copyright 2022 Datafuse Labs.
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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_users::UserApiProvider;

use crate::interpreters::InsertInterpreterV2;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalScalarBuilder;
use crate::sql::plans::create_table_v2::CreateTablePlanV2;
use crate::sql::plans::insert::Insert;
use crate::sql::plans::insert::InsertInputSource;
use crate::sql::plans::Plan;
use crate::storages::StorageDescription;

pub struct CreateTableInterpreterV2 {
    ctx: Arc<QueryContext>,
    plan: CreateTablePlanV2,
}

impl CreateTableInterpreterV2 {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTablePlanV2) -> Result<Self> {
        Ok(CreateTableInterpreterV2 { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTableInterpreterV2 {
    fn name(&self) -> &str {
        "CreateTableInterpreterV2"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.plan.tenant.clone();
        let quota_api = UserApiProvider::instance().get_tenant_quota_api_client(&tenant)?;
        let quota = quota_api.get_quota(None).await?.data;
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

impl CreateTableInterpreterV2 {
    async fn create_table_as_select(&self, select_plan: Box<Plan>) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(&self.plan.catalog)?;

        // TODO: maybe the table creation and insertion should be a transaction, but it may require create_table support 2pc.
        catalog.create_table(self.build_request()?).await?;
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
        let table_schema = table.schema();
        let select_fields: Vec<DataField> = select_plan
            .schema()
            .fields()
            .iter()
            .filter_map(|f| table_schema.field_with_name(f.name()).ok())
            .cloned()
            .collect();
        let schema = DataSchemaRefExt::create(select_fields);
        let insert_plan = Insert {
            catalog: self.plan.catalog.clone(),
            database: self.plan.database.clone(),
            table: self.plan.table.clone(),
            table_id: table.get_id(),
            schema,
            overwrite: false,
            source: InsertInputSource::SelectPlan(select_plan),
        };

        InsertInterpreterV2::try_create(self.ctx.clone(), insert_plan, false)?
            .execute2()
            .await
    }

    async fn create_table(&self) -> Result<PipelineBuildResult> {
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str())?;
        catalog.create_table(self.build_request()?).await?;

        Ok(PipelineBuildResult::create())
    }

    /// Build CreateTableReq from CreateTablePlanV2.
    ///
    /// - Rebuild `DataSchema` with default exprs.
    /// - Update cluster key of table meta.
    fn build_request(&self) -> Result<CreateTableReq> {
        let mut fields = Vec::with_capacity(self.plan.schema.num_fields());
        for (idx, field) in self.plan.schema.fields().clone().into_iter().enumerate() {
            let field = if let Some(scalar) = &self.plan.field_default_exprs[idx] {
                let mut builder = PhysicalScalarBuilder;
                let physical_scaler = builder.build(scalar)?;
                field.with_default_expr(Some(serde_json::to_string(&physical_scaler)?))
            } else {
                field
            };
            fields.push(field)
        }
        let schema = DataSchemaRefExt::create(fields);

        let mut table_meta = TableMeta {
            schema,
            engine: self.plan.engine.to_string(),
            options: self.plan.options.clone(),
            default_cluster_key: None,
            field_comments: self.plan.field_comments.clone(),
            drop_on: None,
            statistics: Default::default(),
            ..Default::default()
        };
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
