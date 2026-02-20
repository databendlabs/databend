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
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::ProcedureIdentity as AstProcedureIdentity;
use databend_common_ast::ast::ProcedureLanguage;
use databend_common_ast::ast::ProcedureType;
use databend_common_ast::ast::ShowOptions;
use databend_common_ast::ast::TypeName;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::ParseMode;
use databend_common_ast::parser::expr::type_name as parse_type_name_ast;
use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::ScriptBlockOrStmt;
use databend_common_ast::parser::script::script_block_or_stmt;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::GetProcedureReply;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::principal::ProcedureIdentity;
use databend_common_meta_app::principal::ProcedureMeta;
use databend_common_meta_app::principal::ProcedureNameIdent;
use databend_common_meta_app::principal::procedure::ProcedureInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;

use crate::BindContext;
use crate::Binder;
use crate::ScalarExpr;
use crate::TypeChecker;
use crate::binder::show::get_show_options;
use crate::meta_service_error;
use crate::plans::CallProcedurePlan;
use crate::plans::ConstantExpr;
use crate::plans::CreateProcedurePlan;
use crate::plans::DescProcedurePlan;
use crate::plans::DropProcedurePlan;
use crate::plans::ExecuteImmediatePlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::plans::SubqueryType;
use crate::resolve_type_name;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_execute_immediate(
        &mut self,
        stmt: &ExecuteImmediateStmt,
    ) -> Result<Plan> {
        let ExecuteImmediateStmt { script } = stmt;
        let script = self.bind_expr(script)?;
        let script = match script {
            ScalarExpr::ConstantExpr(ConstantExpr {
                value: Scalar::String(value),
                ..
            }) => value,
            _ => {
                return Err(ErrorCode::InvalidArgument(
                    "immediate script must be a string",
                ));
            }
        };

        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        let tokens = tokenize_sql(&script)?;
        let ast = run_parser(
            &tokens,
            sql_dialect,
            ParseMode::Template,
            false,
            script_block_or_stmt,
        )?;

        match ast {
            ScriptBlockOrStmt::ScriptBlock(script_block) => {
                Ok(Plan::ExecuteImmediate(Box::new(ExecuteImmediatePlan {
                    script_block,
                    script,
                })))
            }
            ScriptBlockOrStmt::Statement(stmt) => {
                let binder = self.clone();
                binder.bind(&stmt).await
            }
        }
    }

    pub async fn bind_create_procedure(&mut self, stmt: &CreateProcedureStmt) -> Result<Plan> {
        let CreateProcedureStmt {
            create_option,
            name,
            language,
            args,
            return_type,
            comment,
            script,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        // TODO:
        // 1. need parser name: ProcedureNameIdent = name + args
        // 2. need check script's return type and stmt.return_type

        let meta = self.procedure_meta(return_type, script, comment, language, args)?;

        Ok(Plan::CreateProcedure(Box::new(CreateProcedurePlan {
            create_option: create_option.clone().into(),
            tenant: tenant.to_owned(),
            name: generate_procedure_name_ident(&tenant, name)?,
            meta,
        })))
    }

    pub async fn bind_drop_procedure(&mut self, stmt: &DropProcedureStmt) -> Result<Plan> {
        let DropProcedureStmt { name, if_exists } = stmt;

        let tenant = self.ctx.get_tenant();
        Ok(Plan::DropProcedure(Box::new(DropProcedurePlan {
            if_exists: *if_exists,
            tenant: tenant.to_owned(),
            name: generate_procedure_name_ident(&tenant, name)?,
            old_name: ProcedureNameIdent::new(tenant, ProcedureIdentity::from(name.clone())),
        })))
    }

    pub async fn bind_desc_procedure(&mut self, stmt: &DescProcedureStmt) -> Result<Plan> {
        let DescProcedureStmt { name } = stmt;

        let tenant = self.ctx.get_tenant();
        Ok(Plan::DescProcedure(Box::new(DescProcedurePlan {
            tenant: tenant.to_owned(),
            name: generate_procedure_name_ident(&tenant, name)?,
        })))
    }

    pub async fn bind_show_procedures(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        let query = format!(
            "SELECT name, procedure_id, arguments, comment, description, created_on FROM default.system.procedures {} ORDER BY name {}",
            show_limit, limit_str,
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowProcedures)
            .await
    }

    pub async fn bind_call_procedure(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CallProcedureStmt,
    ) -> Result<Plan> {
        let CallProcedureStmt {
            name,
            args: arguments,
        } = stmt;
        let tenant = self.ctx.get_tenant();
        let mut type_checker = TypeChecker::try_create(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            true,
        )?;
        let mut arg_types = Vec::with_capacity(arguments.len());
        for argument in arguments {
            let box (arg, mut arg_type) = type_checker.resolve(argument)?;
            if let ScalarExpr::SubqueryExpr(subquery) = &arg {
                if subquery.typ == SubqueryType::Scalar && !arg.data_type()?.is_nullable() {
                    arg_type = arg_type.wrap_nullable();
                }
            }
            arg_types.push(arg_type);
        }

        let name_str = name.to_string();
        let procedure_api = UserApiProvider::instance().procedure_api(&tenant);

        // Try exact match first
        let arg_type_strings: Vec<String> = arg_types.iter().map(|t| t.to_string()).collect();
        let procedure_ident = ProcedureIdentity::new(name_str.clone(), arg_type_strings.join(","));
        let req = GetProcedureReq {
            inner: ProcedureNameIdent::new(tenant.clone(), procedure_ident.clone()),
        };
        if let Some(procedure) = procedure_api
            .get_procedure(&req)
            .await
            .map_err(meta_service_error)?
        {
            return Ok(Plan::CallProcedure(Box::new(CallProcedurePlan {
                procedure_id: procedure.id,
                script: procedure.procedure_meta.script,
                arg_names: procedure.procedure_meta.arg_names,
                args: arguments.clone(),
            })));
        }

        // Exact match failed, try implicit cast resolution.
        let candidates = procedure_api
            .list_procedures_by_name(&name_str)
            .await
            .map_err(meta_service_error)?;
        let mut same_arity_candidates = Vec::new();
        for candidate in candidates {
            let arg_defs = parse_procedure_signature(&candidate.name_ident.procedure_name().args)?;
            if arg_defs.len() == arg_types.len() {
                same_arity_candidates.push(candidate);
            }
        }

        let has_explicit_cast = arguments
            .iter()
            .any(|expr| matches!(expr, Expr::Cast { .. } | Expr::TryCast { .. }));

        // Multiple overloads plus lack of explicit casts means we cannot disambiguate.
        if same_arity_candidates.len() > 1 && !has_explicit_cast {
            return Err(ErrorCode::UnknownProcedure(format!(
                "Unknown procedure {}",
                procedure_ident
            )));
        }

        let allow_implicit_cast = same_arity_candidates.len() == 1 && !has_explicit_cast;
        let (procedure, casts_to_apply) = resolve_procedure_candidate(
            &procedure_ident,
            &arg_types,
            same_arity_candidates,
            allow_implicit_cast,
        )?;

        let args = arguments
            .iter()
            .zip(casts_to_apply.into_iter())
            .map(|(expr, cast)| match cast {
                Some(target_type) => Expr::Cast {
                    span: expr.span(),
                    expr: Box::new(expr.clone()),
                    target_type,
                    pg_style: false,
                },
                None => expr.clone(),
            })
            .collect();
        Ok(Plan::CallProcedure(Box::new(CallProcedurePlan {
            procedure_id: procedure.id,
            script: procedure.procedure_meta.script,
            arg_names: procedure.procedure_meta.arg_names,
            args,
        })))
    }

    fn procedure_meta(
        &self,
        return_type: &[ProcedureType],
        script: &str,
        comment: &Option<String>,
        language: &ProcedureLanguage,
        args: &Option<Vec<ProcedureType>>,
    ) -> Result<ProcedureMeta> {
        let mut arg_names = vec![];
        if let Some(args) = args {
            for arg in args {
                if let Some(name) = &arg.name {
                    arg_names.push(name.to_string());
                }
            }
        }
        let mut return_types = Vec::with_capacity(return_type.len());
        for arg_type in return_type {
            return_types.push(DataType::from(&resolve_type_name(
                &arg_type.data_type,
                true,
            )?));
        }

        Ok(ProcedureMeta {
            return_types,
            arg_names,
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

pub(in crate::planner::binder) fn generate_procedure_name_ident(
    tenant: &Tenant,
    name: &AstProcedureIdentity,
) -> Result<ProcedureNameIdent> {
    if name.args_type.is_empty() {
        return Ok(ProcedureNameIdent::new(tenant, name.clone().into()));
    }

    let args_data_type: Vec<DataType> = name
        .args_type
        .iter()
        .map(|type_name| resolve_type_name(type_name, true).map(|t| DataType::from(&t)))
        .collect::<Result<Vec<_>, _>>()?;

    let args_type_str = args_data_type
        .iter()
        .map(|dt| dt.to_string())
        .collect::<Vec<_>>()
        .join(",");

    Ok(ProcedureNameIdent::new(
        tenant,
        ProcedureIdentity::new(name.name.clone(), args_type_str),
    ))
}

/// Find the first procedure overload whose signature is compatible with the
/// actual argument types, optionally allowing implicit casts. Returns the
/// procedure metadata plus the exact casts we need to inject.
fn resolve_procedure_candidate(
    procedure_ident: &ProcedureIdentity,
    arg_types: &[DataType],
    candidates: Vec<ProcedureInfo>,
    allow_implicit_cast: bool,
) -> Result<(GetProcedureReply, Vec<Option<TypeName>>)> {
    for candidate in candidates {
        let arg_defs = parse_procedure_signature(&candidate.name_ident.procedure_name().args)?;
        if arg_defs.len() != arg_types.len() {
            continue;
        }

        let mut casts = Vec::with_capacity(arg_types.len());
        let mut compatible = true;
        for (actual, target_ast) in arg_types.iter().zip(arg_defs.iter()) {
            let target = DataType::from(&resolve_type_name(target_ast, true)?);
            if actual == &target {
                casts.push(None);
                continue;
            }

            if allow_implicit_cast
                && common_super_type(
                    actual.clone(),
                    target.clone(),
                    &BUILTIN_FUNCTIONS.default_cast_rules,
                )
                .is_some_and(|common| common == target)
            {
                casts.push(Some(target_ast.clone()));
            } else {
                compatible = false;
                break;
            }
        }

        if compatible {
            return Ok((
                GetProcedureReply {
                    id: *candidate.ident.procedure_id(),
                    procedure_meta: candidate.meta.clone(),
                },
                casts,
            ));
        }
    }

    Err(ErrorCode::UnknownProcedure(format!(
        "Unknown procedure {}",
        procedure_ident
    )))
}

fn parse_procedure_signature(arg_str: &str) -> Result<Vec<TypeName>> {
    if arg_str.is_empty() {
        return Ok(vec![]);
    }

    let mut segments = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (idx, ch) in arg_str.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                // Only split on commas at depth 0 so nested args (like DECIMAL) stay intact.
                segments.push(arg_str[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }
    segments.push(arg_str[start..].trim());

    segments
        .into_iter()
        .map(|segment| {
            let tokens = tokenize_sql(segment)?;
            run_parser(
                &tokens,
                Dialect::default(),
                ParseMode::Default,
                false,
                parse_type_name_ast,
            )
            .map_err(|e| ErrorCode::SyntaxException(e.to_string()))
        })
        .collect()
}
