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

use std::any::Any;
use std::sync::Arc;

use databend_common_ast::parser::parse_database_ref;
use databend_common_ast::parser::parse_table_ref;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::string_value;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::tag_api::TagApi;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TagIdToNameIdent;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::tag::id_ident::TagId;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::processor::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_sql::planner::NameResolutionContext;
use databend_common_sql::planner::normalize_identifier;
use databend_common_users::UserApiProvider;

const TAG_REFERENCES_FUNC: &str = "tag_references";
const TAG_REFERENCES_ENGINE: &str = "TAG_REFERENCES";

pub struct TagReferencesTable {
    table_info: TableInfo,
    args: TableArgs,
}

impl TagReferencesTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: TAG_REFERENCES_ENGINE.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            args: table_args,
        }))
    }

    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("tag_name", TableDataType::String),
            TableField::new("tag_value", TableDataType::String),
            TableField::new(
                "object_database",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "object_id",
                TableDataType::Nullable(Box::new(TableDataType::Number(
                    databend_common_expression::types::NumberDataType::UInt64,
                ))),
            ),
            TableField::new("object_name", TableDataType::String),
            TableField::new("domain", TableDataType::String),
        ])
    }
}

#[async_trait::async_trait]
impl Table for TagReferencesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                TagReferencesSource::create(
                    ctx.clone(),
                    output,
                    self.args.clone(),
                    self.table_info.meta.schema.clone(),
                )
            },
            1,
        )?;
        Ok(())
    }
}

impl TableFunction for TagReferencesTable {
    fn function_name(&self) -> &str {
        TAG_REFERENCES_FUNC
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct TagReferencesSource {
    ctx: Arc<dyn TableContext>,
    finished: bool,
    args: TableArgs,
    schema: DataSchemaRef,
}

impl TagReferencesSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args: TableArgs,
        schema: Arc<TableSchema>,
    ) -> Result<ProcessorPtr> {
        let data_schema = Arc::new(DataSchema::from(schema.as_ref()));
        AsyncSourcer::create(ctx.get_scan_progress(), output, TagReferencesSource {
            ctx,
            finished: false,
            args,
            schema: data_schema,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for TagReferencesSource {
    const NAME: &'static str = "tag_references";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        let block =
            collect_tag_references(self.ctx.clone(), self.args.clone(), self.schema.clone())
                .await?;
        self.finished = true;
        Ok(Some(block))
    }
}

async fn collect_tag_references(
    ctx: Arc<dyn TableContext>,
    table_args: TableArgs,
    schema: DataSchemaRef,
) -> Result<DataBlock> {
    let args = table_args.expect_all_positioned(TAG_REFERENCES_FUNC, Some(2))?;

    let object_name = string_value(&args[0])?;
    let object_domain = string_value(&args[1])?;

    let tenant = ctx.get_tenant();
    let meta = UserApiProvider::instance().get_meta_store_client();

    let domain_upper = object_domain.trim().to_ascii_uppercase();

    let (taggable_object, obj_db, obj_id, obj_name) = match domain_upper.as_str() {
        "DATABASE" => {
            let (catalog_name, db_name) = parse_database_name(&ctx, &object_name)?;
            let catalog = ctx.get_catalog(&catalog_name).await?;
            let db = catalog.get_database(&tenant, &db_name).await?;
            let db_id = db.get_db_info().database_id.db_id;
            (
                TaggableObject::Database { db_id },
                Some(db_name.to_string()),
                Some(db_id),
                db_name.to_string(),
            )
        }
        "TABLE" => {
            let (catalog_name, db_name, table_name) = parse_table_name(&ctx, &object_name)?;
            let catalog = ctx.get_catalog(&catalog_name).await?;
            let table = catalog.get_table(&tenant, &db_name, &table_name).await?;
            let table_id = table.get_table_info().ident.table_id;
            (
                TaggableObject::Table { table_id },
                Some(db_name),
                Some(table_id),
                table_name,
            )
        }
        "STAGE" => {
            let stage_name = object_name.trim();
            // Validate stage exists
            UserApiProvider::instance()
                .get_stage(&tenant, stage_name)
                .await?;
            (
                TaggableObject::Stage {
                    name: stage_name.to_string(),
                },
                None,
                None,
                stage_name.to_string(),
            )
        }
        "CONNECTION" => {
            let conn_name = object_name.trim();
            // Validate connection exists
            UserApiProvider::instance()
                .get_connection(&tenant, conn_name)
                .await?;
            (
                TaggableObject::Connection {
                    name: conn_name.to_string(),
                },
                None,
                None,
                conn_name.to_string(),
            )
        }
        _ => {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid object_domain '{}'. Supported values: DATABASE, TABLE, STAGE, CONNECTION",
                object_domain
            )));
        }
    };

    // Get object tags
    let tags = meta.get_object_tags(&tenant, &taggable_object).await?;

    if tags.is_empty() {
        return Ok(DataBlock::empty_with_schema(schema));
    }

    // Batch fetch tag names
    let tag_id_to_name_keys: Vec<TagIdToNameIdent> = tags
        .iter()
        .map(|t| TagIdToNameIdent::new_generic(tenant.clone(), TagId::new(t.tag_id)))
        .collect();

    let tag_names_result = meta.get_pb_values_vec(tag_id_to_name_keys).await?;

    let len = tags.len();
    let mut tag_names = Vec::with_capacity(len);
    let mut tag_values = Vec::with_capacity(len);
    let obj_databases: Vec<Option<String>> = vec![obj_db; len];
    let obj_ids: Vec<Option<u64>> = vec![obj_id; len];
    let obj_names: Vec<String> = vec![obj_name; len];
    let domains: Vec<String> = vec![domain_upper; len];

    for (tag, name_opt) in tags.iter().zip(tag_names_result.into_iter()) {
        let name = name_opt
            .ok_or_else(|| ErrorCode::UnknownTag(format!("Unknown tag_id: {}", tag.tag_id)))?
            .data
            .tag_name()
            .to_string();
        tag_names.push(name);
        tag_values.push(tag.tag_value.data.tag_allowed_value.clone());
    }

    Ok(DataBlock::new_from_columns(vec![
        StringType::from_data(tag_names),
        StringType::from_data(tag_values),
        StringType::from_opt_data(obj_databases),
        UInt64Type::from_opt_data(obj_ids),
        StringType::from_data(obj_names),
        StringType::from_data(domains),
    ]))
}

/// Parse database name in format "db" or "catalog.db".
/// Correctly handles quoted identifiers and normalizes them according to session settings.
/// Returns (catalog, database).
fn parse_database_name(ctx: &Arc<dyn TableContext>, name: &str) -> Result<(String, String)> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ErrorCode::BadArguments("object_name must not be empty"));
    }

    let settings = ctx.get_settings();
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let dialect = settings.get_sql_dialect().unwrap_or_default();

    let db_ref = parse_database_ref(trimmed, dialect).map_err(|e| {
        ErrorCode::BadArguments(format!("Invalid database name '{}': {}", name, e.1))
    })?;

    let catalog = db_ref
        .catalog
        .map(|i| normalize_identifier(&i, &name_resolution_ctx).name)
        .unwrap_or_else(|| ctx.get_current_catalog());
    let database = normalize_identifier(&db_ref.database, &name_resolution_ctx).name;

    Ok((catalog, database))
}

/// Parse table name in format "table", "db.table", or "catalog.db.table".
/// Correctly handles quoted identifiers and normalizes them according to session settings.
/// Returns (catalog, database, table).
fn parse_table_name(ctx: &Arc<dyn TableContext>, name: &str) -> Result<(String, String, String)> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ErrorCode::BadArguments("object_name must not be empty"));
    }

    let settings = ctx.get_settings();
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let dialect = settings.get_sql_dialect().unwrap_or_default();

    let table_ref = parse_table_ref(trimmed, dialect)
        .map_err(|e| ErrorCode::BadArguments(format!("Invalid table name '{}': {}", name, e.1)))?;

    let catalog = table_ref
        .catalog
        .map(|i| normalize_identifier(&i, &name_resolution_ctx).name)
        .unwrap_or_else(|| ctx.get_current_catalog());
    let database = table_ref
        .database
        .map(|i| normalize_identifier(&i, &name_resolution_ctx).name)
        .unwrap_or_else(|| ctx.get_current_database());
    let table = normalize_identifier(&table_ref.table, &name_resolution_ctx).name;

    Ok((catalog, database, table))
}
