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
use std::collections::HashMap;
use std::sync::Arc;

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
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::name_id_value_api::NameIdValueApi;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::schema::SecurityPolicyColumnMap;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::processor::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_users::UserApiProvider;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::ListOptions;

use crate::meta_service_error;

const POLICY_REFERENCES_FUNC: &str = "policy_references";
const POLICY_REFERENCES_ENGINE: &str = "POLICY_REFERENCES";
const MASKING_POLICY_KIND: &str = "MASKING POLICY";
const ROW_ACCESS_POLICY_KIND: &str = "ROW ACCESS POLICY";
const POLICY_STATUS_ACTIVE: &str = "ACTIVE";
const ARGUMENT_ERROR_MESSAGE: &str = "function 'policy_references' expects argument (policy_name=>'policyName') or (ref_entity_name=>'name', ref_entity_domain=>'domain')";

#[derive(Clone)]
struct PolicyReferenceRow {
    policy_name: String,
    policy_kind: String,
    ref_database: String,
    ref_entity: String,
    ref_entity_domain: String,
    ref_column_name: Option<String>,
    ref_arg_column_names: Option<String>,
}

#[derive(Clone, Copy)]
enum PolicyKind {
    Masking,
    RowAccess,
}

pub struct PolicyReferencesTable {
    table_info: TableInfo,
    args: TableArgs,
}

impl PolicyReferencesTable {
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
                engine: POLICY_REFERENCES_ENGINE.to_string(),
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
            TableField::new("policy_name", TableDataType::String),
            TableField::new("policy_kind", TableDataType::String),
            TableField::new("ref_database_name", TableDataType::String),
            TableField::new("ref_entity_name", TableDataType::String),
            TableField::new("ref_entity_domain", TableDataType::String),
            TableField::new(
                "ref_column_name",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "ref_arg_column_names",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("policy_status", TableDataType::String),
        ])
    }
}

#[async_trait::async_trait]
impl Table for PolicyReferencesTable {
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
                PolicyReferencesSource::create(
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

impl TableFunction for PolicyReferencesTable {
    fn function_name(&self) -> &str {
        POLICY_REFERENCES_FUNC
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct PolicyReferencesSource {
    ctx: Arc<dyn TableContext>,
    finished: bool,
    args: TableArgs,
    schema: DataSchemaRef,
}

impl PolicyReferencesSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args: TableArgs,
        schema: Arc<TableSchema>,
    ) -> Result<ProcessorPtr> {
        let data_schema = Arc::new(DataSchema::from(schema.as_ref()));
        AsyncSourcer::create(ctx.get_scan_progress(), output, PolicyReferencesSource {
            ctx,
            finished: false,
            args,
            schema: data_schema,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for PolicyReferencesSource {
    const NAME: &'static str = "policy_references";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        let rows = collect_policy_reference_rows(self.ctx.clone(), self.args.clone()).await?;
        let block = if rows.is_empty() {
            DataBlock::empty_with_schema(&self.schema)
        } else {
            let mut policy_names = Vec::with_capacity(rows.len());
            let mut policy_kinds = Vec::with_capacity(rows.len());
            let mut ref_databases = Vec::with_capacity(rows.len());
            let mut ref_entities = Vec::with_capacity(rows.len());
            let mut ref_domains = Vec::with_capacity(rows.len());
            let mut ref_columns = Vec::with_capacity(rows.len());
            let mut ref_args = Vec::with_capacity(rows.len());
            let mut statuses = Vec::with_capacity(rows.len());

            for row in &rows {
                policy_names.push(row.policy_name.clone());
                policy_kinds.push(row.policy_kind.clone());
                ref_databases.push(row.ref_database.clone());
                ref_entities.push(row.ref_entity.clone());
                ref_domains.push(row.ref_entity_domain.clone());
                ref_columns.push(row.ref_column_name.clone());
                ref_args.push(row.ref_arg_column_names.clone());
                statuses.push(String::from(POLICY_STATUS_ACTIVE));
            }

            DataBlock::new_from_columns(vec![
                StringType::from_data(policy_names),
                StringType::from_data(policy_kinds),
                StringType::from_data(ref_databases),
                StringType::from_data(ref_entities),
                StringType::from_data(ref_domains),
                StringType::from_opt_data(ref_columns),
                StringType::from_opt_data(ref_args),
                StringType::from_data(statuses),
            ])
        };

        self.finished = true;
        Ok(Some(block))
    }
}

async fn collect_policy_reference_rows(
    ctx: Arc<dyn TableContext>,
    table_args: TableArgs,
) -> Result<Vec<PolicyReferenceRow>> {
    let args = table_args.expect_all_named(POLICY_REFERENCES_FUNC)?;
    if args.is_empty() {
        return Err(ErrorCode::BadArguments(ARGUMENT_ERROR_MESSAGE));
    }

    let mut policy_name = None;
    let mut ref_entity_name = None;
    let mut ref_entity_domain = None;

    for (key, value) in args.iter() {
        match key.to_lowercase().as_str() {
            "policy_name" => policy_name = Some(string_value(value)?),
            "ref_entity_name" => ref_entity_name = Some(string_value(value)?),
            "ref_entity_domain" => {
                let raw = string_value(value)?;
                let upper = raw.trim().to_ascii_uppercase();
                match upper.as_str() {
                    "TABLE" | "VIEW" => ref_entity_domain = Some(upper),
                    _ => {
                        return Err(ErrorCode::BadArguments(format!(
                            "Invalid REF_ENTITY_DOMAIN '{}'. Expected TABLE or VIEW",
                            raw
                        )));
                    }
                }
            }
            _ => {
                return Err(ErrorCode::BadArguments(format!(
                    "{} invalid argument '{}'",
                    POLICY_REFERENCES_FUNC, key
                )));
            }
        }
    }

    let tenant = ctx.get_tenant();
    let catalog = ctx.get_default_catalog()?;
    let meta = UserApiProvider::instance().get_meta_store_client();

    match (policy_name, ref_entity_name, ref_entity_domain) {
        (Some(name), None, None) => {
            let mask_ident = DataMaskNameIdent::new(&tenant, &name);
            let (policy_id, kind) = if let Some(seq_id) =
                meta.get_pb(&mask_ident).await.map_err(meta_service_error)?
            {
                (*seq_id.data, PolicyKind::Masking)
            } else {
                let row_ident = RowAccessPolicyNameIdent::new(&tenant, &name);
                if let Some(seq_id) = meta.get_pb(&row_ident).await.map_err(meta_service_error)? {
                    (*seq_id.data, PolicyKind::RowAccess)
                } else {
                    return Ok(vec![]);
                }
            };

            let table_ids = match kind {
                PolicyKind::Masking => {
                    let ident =
                        MaskPolicyTableIdIdent::new_generic(tenant.clone(), MaskPolicyIdTableId {
                            policy_id,
                            table_id: 0,
                        });
                    meta.list_pb_vec(ListOptions::unlimited(&DirName::new(ident)))
                        .await
                        .map_err(meta_service_error)?
                        .into_iter()
                        .map(|(key, _)| key.name().table_id)
                        .collect::<Vec<_>>()
                }
                PolicyKind::RowAccess => {
                    let ident = RowAccessPolicyTableIdIdent::new_generic(
                        tenant.clone(),
                        RowAccessPolicyIdTableId {
                            policy_id,
                            table_id: 0,
                        },
                    );
                    meta.list_pb_vec(ListOptions::unlimited(&DirName::new(ident)))
                        .await
                        .map_err(meta_service_error)?
                        .into_iter()
                        .map(|(key, _)| key.name().table_id)
                        .collect::<Vec<_>>()
                }
            };

            let mut rows = Vec::new();
            for table_id in table_ids {
                let Some(seq_meta) = catalog.get_table_meta_by_id(table_id).await? else {
                    continue;
                };

                let id_to_name_key = TableIdToName { table_id };
                let Some(name_entry) = meta
                    .get_pb(&id_to_name_key)
                    .await
                    .map_err(meta_service_error)?
                else {
                    continue;
                };

                let db_name = catalog.get_db_name_by_id(name_entry.data.db_id).await?;
                let table_name = name_entry.data.table_name;
                let domain = if seq_meta.data.engine.eq_ignore_ascii_case("VIEW") {
                    "VIEW".to_string()
                } else {
                    "TABLE".to_string()
                };
                let column_map = seq_meta
                    .data
                    .schema
                    .fields()
                    .iter()
                    .map(|f| (f.column_id, f.name().to_string()))
                    .collect::<HashMap<_, _>>();

                match kind {
                    PolicyKind::Masking => {
                        for map in seq_meta.data.column_mask_policy_columns_ids.values() {
                            if map.policy_id != policy_id {
                                continue;
                            }
                            if let Some(row) = build_mask_row(
                                &name,
                                &db_name,
                                &table_name,
                                &domain,
                                &column_map,
                                map,
                            ) {
                                rows.push(row);
                            }
                        }
                    }
                    PolicyKind::RowAccess => {
                        if let Some(policy_map) = &seq_meta.data.row_access_policy_columns_ids {
                            if policy_map.policy_id == policy_id {
                                if let Some(row) = build_row_access_row(
                                    &name,
                                    &db_name,
                                    &table_name,
                                    &domain,
                                    &column_map,
                                    policy_map,
                                ) {
                                    rows.push(row);
                                }
                            }
                        }
                    }
                }
            }

            Ok(rows)
        }
        (None, Some(name), Some(domain)) => {
            let default_db = ctx.get_current_database();
            let trimmed = name.trim();
            if trimmed.is_empty() {
                return Err(ErrorCode::BadArguments(
                    "REF_ENTITY_NAME must not be empty".to_string(),
                ));
            }
            let (db_name, table_name) = if let Some((db, table)) = trimmed.split_once('.') {
                let tbl = table.trim();
                if tbl.is_empty() {
                    return Err(ErrorCode::BadArguments(format!(
                        "Invalid REF_ENTITY_NAME '{}'",
                        name
                    )));
                }
                (db.trim().to_string(), tbl.to_string())
            } else {
                (default_db, trimmed.to_string())
            };
            let catalog_name = ctx.get_current_catalog();
            let table = ctx.get_table(&catalog_name, &db_name, &table_name).await?;
            let actual_domain = if table.engine().eq_ignore_ascii_case("VIEW") {
                "VIEW".to_string()
            } else {
                "TABLE".to_string()
            };

            if actual_domain != domain {
                return Err(ErrorCode::BadArguments(format!(
                    "Object '{}' is a {}, but argument REF_ENTITY_DOMAIN specified {}",
                    name, actual_domain, domain
                )));
            }

            let table_info = table.get_table_info();
            let column_map = table_info
                .meta
                .schema
                .fields()
                .iter()
                .map(|f| (f.column_id, f.name().to_string()))
                .collect::<HashMap<_, _>>();

            let mask_names = {
                let ident = DataMaskNameIdent::new(&tenant, "");
                meta.list_id_value(&DirName::new(ident))
                    .await
                    .map_err(meta_service_error)?
                    .map(|(key, id_seqv, _)| (*id_seqv.data, key.data_mask_name().to_string()))
                    .collect::<HashMap<_, _>>()
            };

            let row_names = {
                let ident = RowAccessPolicyNameIdent::new(&tenant, "");
                meta.list_id_value(&DirName::new(ident))
                    .await
                    .map_err(meta_service_error)?
                    .map(|(key, id_seqv, _)| (*id_seqv.data, key.row_access_name().to_string()))
                    .collect::<HashMap<_, _>>()
            };

            let mut rows = Vec::new();

            for map in table_info.meta.column_mask_policy_columns_ids.values() {
                let policy_name = mask_names
                    .get(&map.policy_id)
                    .cloned()
                    .unwrap_or_else(|| map.policy_id.to_string());
                if let Some(row) = build_mask_row(
                    &policy_name,
                    &db_name,
                    &table_name,
                    &actual_domain,
                    &column_map,
                    map,
                ) {
                    rows.push(row);
                }
            }

            if let Some(policy_map) = &table_info.meta.row_access_policy_columns_ids {
                let policy_name = row_names
                    .get(&policy_map.policy_id)
                    .cloned()
                    .unwrap_or_else(|| policy_map.policy_id.to_string());
                if let Some(row) = build_row_access_row(
                    &policy_name,
                    &db_name,
                    &table_name,
                    &actual_domain,
                    &column_map,
                    policy_map,
                ) {
                    rows.push(row);
                }
            }

            Ok(rows)
        }
        _ => Err(ErrorCode::BadArguments(ARGUMENT_ERROR_MESSAGE)),
    }
}

fn build_mask_row(
    policy_name: &str,
    db_name: &str,
    table_name: &str,
    domain: &str,
    column_map: &HashMap<u32, String>,
    map: &SecurityPolicyColumnMap,
) -> Option<PolicyReferenceRow> {
    if map.columns_ids.is_empty() {
        return None;
    }

    let column_names = map
        .columns_ids
        .iter()
        .filter_map(|id| column_map.get(id).cloned())
        .collect::<Vec<_>>();

    let ref_column_name = column_names.first().cloned();
    let arg_columns = if column_names.len() > 1 {
        Some(column_names[1..].join(", "))
    } else {
        None
    };

    Some(PolicyReferenceRow {
        policy_name: policy_name.to_string(),
        policy_kind: MASKING_POLICY_KIND.to_string(),
        ref_database: db_name.to_string(),
        ref_entity: table_name.to_string(),
        ref_entity_domain: domain.to_string(),
        ref_column_name,
        ref_arg_column_names: arg_columns,
    })
}

fn build_row_access_row(
    policy_name: &str,
    db_name: &str,
    table_name: &str,
    domain: &str,
    column_map: &HashMap<u32, String>,
    map: &SecurityPolicyColumnMap,
) -> Option<PolicyReferenceRow> {
    let column_names = map
        .columns_ids
        .iter()
        .filter_map(|id| column_map.get(id).cloned())
        .collect::<Vec<_>>();

    Some(PolicyReferenceRow {
        policy_name: policy_name.to_string(),
        policy_kind: ROW_ACCESS_POLICY_KIND.to_string(),
        ref_database: db_name.to_string(),
        ref_entity: table_name.to_string(),
        ref_entity_domain: domain.to_string(),
        ref_column_name: None,
        ref_arg_column_names: if column_names.is_empty() {
            None
        } else {
            Some(column_names.join(", "))
        },
    })
}
