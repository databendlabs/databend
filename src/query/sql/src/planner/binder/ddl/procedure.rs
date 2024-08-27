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

use chrono::Utc;
use databend_common_ast::ast::CallProcedureStmt;
use databend_common_ast::ast::CreateProcedureStmt;
use databend_common_ast::ast::DescProcedureStmt;
use databend_common_ast::ast::DropProcedureStmt;
use databend_common_ast::ast::ExecuteImmediateStmt;
use databend_common_ast::ast::ProcedureLanguage;
use databend_common_ast::ast::ProcedureReturnType;
use databend_common_ast::ast::ShowOptions;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::principal::ProcedureMeta;
use databend_common_meta_app::principal::ProcedureNameIdent;
use databend_common_users::UserApiProvider;

use crate::binder::show::get_show_options;
use crate::plans::CreateProcedurePlan;
use crate::plans::DropProcedurePlan;
use crate::plans::ExecuteImmediatePlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::resolve_type_name;
use crate::BindContext;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_execute_immediate(
        &mut self,
        stmt: &ExecuteImmediateStmt,
    ) -> Result<Plan> {
        let ExecuteImmediateStmt { script } = stmt;
        Ok(Plan::ExecuteImmediate(Box::new(ExecuteImmediatePlan {
            script: script.clone(),
        })))
    }

    pub async fn bind_create_procedure(&mut self, stmt: &CreateProcedureStmt) -> Result<Plan> {
        let CreateProcedureStmt {
            create_option,
            name,
            language,
            args: _args,
            return_type,
            comment,
            script,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        // TODO:
        // 1. need parser name: ProcedureNameIdent = name + args
        // 2. need check script's return type and stmt.return_type

        let meta = self.procedure_meta(return_type, script, comment, language)?;
        Ok(Plan::CreateProcedure(Box::new(CreateProcedurePlan {
            create_option: create_option.clone().into(),
            tenant,
            name: name.to_owned(),
            meta,
        })))
    }

    pub async fn bind_drop_procedure(&mut self, stmt: &DropProcedureStmt) -> Result<Plan> {
        let DropProcedureStmt { name, args: _args } = stmt;

        let tenant = self.ctx.get_tenant();
        // TODO: need parser name: ProcedureNameIdent = name + args
        Ok(Plan::DropProcedure(Box::new(DropProcedurePlan {
            if_exists: false,
            tenant: tenant.to_owned(),
            name: ProcedureNameIdent::new(tenant, name),
        })))
    }

    pub async fn bind_desc_procedure(&mut self, _stmt: &DescProcedureStmt) -> Result<Plan> {
        todo!()
    }

    pub async fn bind_show_procedures(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        let query = format!(
            "SELECT name, procedure_id, arguments, comment, description, created_on FROM system.procedures {} ORDER BY name {}",
            show_limit, limit_str,
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowProcedures)
            .await
    }

    pub async fn bind_call_procedure(&mut self, stmt: &CallProcedureStmt) -> Result<Plan> {
        let CallProcedureStmt { name, args: _args } = stmt;
        let tenant = self.ctx.get_tenant();
        // TODO: ProcedureNameIdent = name + args_type. Need to get type in here.
        let req = GetProcedureReq {
            inner: ProcedureNameIdent::new(tenant.clone(), name),
        };
        let procedure = UserApiProvider::instance()
            .get_procedure(&tenant, req)
            .await?;
        Ok(Plan::ExecuteImmediate(Box::new(ExecuteImmediatePlan {
            script: procedure.meta.script,
        })))
    }

    fn procedure_meta(
        &self,
        return_type: &[ProcedureReturnType],
        script: &str,
        comment: &Option<String>,
        language: &ProcedureLanguage,
    ) -> Result<ProcedureMeta> {
        let mut return_types = Vec::with_capacity(return_type.len());
        for arg_type in return_type {
            return_types.push(DataType::from(&resolve_type_name(
                &arg_type.data_type,
                true,
            )?));
        }

        Ok(ProcedureMeta {
            return_types,
            created_on: Utc::now(),
            updated_on: Utc::now(),
            script: script.to_string(),
            comment: if let Some(comment) = comment {
                comment.to_owned()
            } else {
                "".to_string()
            },
            procedure_language: match language {
                ProcedureLanguage::SQL => "SQL".to_string(),
            },
        })
    }
}
