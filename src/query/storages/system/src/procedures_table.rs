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

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::utils::FromData;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::principal::ListProcedureReq;
use databend_common_meta_app::principal::ProcedureIdentity;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use itertools::Itertools;

use crate::meta_service_error;
use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

// Response for list procedures API (aligned with SHOW PROCEDURES)
#[derive(serde::Serialize)]
pub struct ProcedureListItem {
    name: String,
    procedure_id: u64,
    arguments: String,
    comment: String,
    description: String,
    created_on: DateTime<Utc>,
}

// Response for get procedure details API (aligned with DESC PROCEDURE)
#[derive(serde::Serialize)]
pub struct ProcedureDetail {
    signature: String,
    returns: String,
    language: String,
    body: String,
}

pub struct ProceduresTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for ProceduresTable {
    const NAME: &'static str = "system.procedures";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let user_api = UserApiProvider::instance();
        let mgr = user_api.procedure_api(&tenant);
        let all_procedures = mgr
            .list_procedures(ListProcedureReq {
                tenant,
                filter: None,
            })
            .await
            .map_err(meta_service_error)?;

        let enable_experimental_rbac_check =
            ctx.get_settings().get_enable_experimental_rbac_check()?;
        let procedures = if enable_experimental_rbac_check {
            let visibility_checker = ctx.get_visibility_checker(false, Object::Procedure).await?;
            all_procedures
                .into_iter()
                .filter(|p| visibility_checker.check_procedure_visibility(&p.ident.procedure_id()))
                .collect::<Vec<_>>()
        } else {
            all_procedures
        };

        let mut names = Vec::with_capacity(procedures.len());
        let mut procedure_ids = Vec::with_capacity(procedures.len());
        let mut languages = Vec::with_capacity(procedures.len());
        let mut descriptions = Vec::with_capacity(procedures.len());
        // TODO: argument = name + arg_type + return_type
        // +------------------------------------+
        // | arguments                          |
        // +------------------------------------+
        // | AREA_OF_CIRCLE(FLOAT) RETURN FLOAT |
        // +------------------------------------+
        let mut arguments = Vec::with_capacity(procedures.len());
        let mut comments = Vec::with_capacity(procedures.len());
        let mut created_on_s = Vec::with_capacity(procedures.len());

        for procedure in &procedures {
            names.push(procedure.name_ident.procedure_name().name.to_string());
            procedure_ids.push(*procedure.ident.procedure_id());
            arguments.push(format!(
                "{} RETURN ({})",
                procedure.name_ident.procedure_name(),
                procedure.meta.return_types.iter().join(",")
            ));
            languages.push(procedure.meta.procedure_language.as_str());
            descriptions.push("user-defined procedure");
            comments.push(procedure.meta.comment.as_str());

            created_on_s.push(procedure.meta.created_on.timestamp_micros());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            UInt64Type::from_data(procedure_ids),
            StringType::from_data(arguments),
            StringType::from_data(comments),
            StringType::from_data(descriptions),
            TimestampType::from_data(created_on_s),
        ]))
    }
}

impl ProceduresTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new(
                "procedure_id",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("arguments", TableDataType::String),
            TableField::new("comment", TableDataType::String),
            TableField::new("description", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'procedures'".to_string(),
            name: "procedures".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemDatabases".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(ProceduresTable { table_info })
    }

    #[async_backtrace::framed]
    pub async fn get_procedures(tenant: &Tenant) -> Result<Vec<ProcedureListItem>> {
        let user_api = UserApiProvider::instance();
        let mgr = user_api.procedure_api(tenant);
        let procedures = mgr
            .list_procedures(ListProcedureReq {
                tenant: tenant.clone(),
                filter: None,
            })
            .await
            .map_err(meta_service_error)?;

        Ok(procedures
            .into_iter()
            .map(|proc_info| ProcedureListItem {
                name: proc_info.name_ident.procedure_name().name.clone(),
                procedure_id: *proc_info.ident.procedure_id(),
                arguments: format!(
                    "{} RETURN ({})",
                    proc_info.name_ident.procedure_name(),
                    proc_info.meta.return_types.iter().join(",")
                ),
                comment: proc_info.meta.comment.clone(),
                description: "user-defined procedure".to_string(),
                created_on: proc_info.meta.created_on,
            })
            .collect())
    }

    #[async_backtrace::framed]
    pub async fn get_procedure(
        tenant: &Tenant,
        name: &str,
        args: &str,
    ) -> Result<Option<ProcedureDetail>> {
        let user_api = UserApiProvider::instance();
        let mgr = user_api.procedure_api(tenant);
        let req = GetProcedureReq::new(tenant, ProcedureIdentity::new(name, args));

        match mgr.get_procedure(&req).await.map_err(meta_service_error)? {
            Some(reply) => {
                // Format signature as (arg1,arg2,...)
                let signature = if reply.procedure_meta.arg_names.is_empty() {
                    "()".to_string()
                } else {
                    format!("({})", reply.procedure_meta.arg_names.join(","))
                };

                // Format returns as (Type1,Type2,...)
                let returns = if reply.procedure_meta.return_types.is_empty() {
                    "()".to_string()
                } else {
                    format!(
                        "({})",
                        reply
                            .procedure_meta
                            .return_types
                            .iter()
                            .map(|t| t.to_string())
                            .join(",")
                    )
                };

                Ok(Some(ProcedureDetail {
                    signature,
                    returns,
                    language: reply.procedure_meta.procedure_language.clone(),
                    body: reply.procedure_meta.script.clone(),
                }))
            }
            None => Ok(None),
        }
    }

    #[async_backtrace::framed]
    pub async fn get_procedure_by_id(
        tenant: &Tenant,
        procedure_id: u64,
    ) -> Result<Option<ProcedureDetail>> {
        let user_api = UserApiProvider::instance();
        let mgr = user_api.procedure_api(tenant);

        match mgr
            .get_procedure_by_id(procedure_id)
            .await
            .map_err(meta_service_error)?
        {
            Some(seq_meta) => {
                let procedure_meta = seq_meta.data;

                // Format signature as (arg1,arg2,...)
                let signature = if procedure_meta.arg_names.is_empty() {
                    "()".to_string()
                } else {
                    format!("({})", procedure_meta.arg_names.join(","))
                };

                // Format returns as (Type1,Type2,...)
                let returns = if procedure_meta.return_types.is_empty() {
                    "()".to_string()
                } else {
                    format!(
                        "({})",
                        procedure_meta
                            .return_types
                            .iter()
                            .map(|t| t.to_string())
                            .join(",")
                    )
                };

                Ok(Some(ProcedureDetail {
                    signature,
                    returns,
                    language: procedure_meta.procedure_language.clone(),
                    body: procedure_meta.script.clone(),
                }))
            }
            None => Ok(None),
        }
    }
}
