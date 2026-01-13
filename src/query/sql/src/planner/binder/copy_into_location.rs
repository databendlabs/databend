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

use databend_common_ast::ast::CopyIntoLocationSource;
use databend_common_ast::ast::CopyIntoLocationStmt;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::quote::display_ident;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storage::init_stage_operator;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::ErrorKind;

use crate::BindContext;
use crate::binder::Binder;
use crate::binder::copy_into_table::resolve_file_location;
use crate::plans::CopyIntoLocationPlan;
use crate::plans::Plan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_copy_into_location(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CopyIntoLocationStmt,
    ) -> Result<Plan> {
        if stmt.options.use_raw_path && !stmt.options.single {
            return Err(ErrorCode::InvalidArgument(
                "use_raw_path=true can only be set when single=true",
            ));
        }
        if stmt.options.overwrite && (!stmt.options.single || !stmt.options.use_raw_path) {
            return Err(ErrorCode::InvalidArgument(
                "overwrite=true can only be set when single=true and use_raw_path=true for now",
            ));
        }
        if !stmt.options.include_query_id && !stmt.options.use_raw_path {
            return Err(ErrorCode::InvalidArgument(
                "include_query_id=false can only be set when use_raw_path=true",
            ));
        }

        let query = match &stmt.src {
            CopyIntoLocationSource::Table {
                table,
                with_options,
            } => {
                let (catalog_name, database_name, table_name, branch_name) =
                    self.normalize_table_ref(table);
                let with_options_str = with_options
                    .as_ref()
                    .map_or(String::new(), |with_options| format!(" {with_options}"));

                let quoted_ident_case_sensitive =
                    self.ctx.get_settings().get_quoted_ident_case_sensitive()?;
                let branch_suffix = branch_name.map_or(String::new(), |branch| {
                    format!(
                        "/{}",
                        display_ident(&branch, false, quoted_ident_case_sensitive, self.dialect)
                    )
                });
                let subquery = format!(
                    "SELECT * FROM {}.{}.{}{branch_suffix}{with_options_str}",
                    display_ident(
                        &catalog_name,
                        false,
                        quoted_ident_case_sensitive,
                        self.dialect
                    ),
                    display_ident(
                        &database_name,
                        false,
                        quoted_ident_case_sensitive,
                        self.dialect
                    ),
                    display_ident(
                        &table_name,
                        false,
                        quoted_ident_case_sensitive,
                        self.dialect
                    ),
                );
                let tokens = tokenize_sql(&subquery)?;
                let sub_stmt_msg = parse_sql(&tokens, self.dialect)?;
                let sub_stmt = sub_stmt_msg.0;
                match &sub_stmt {
                    Statement::Query(query) => {
                        self.bind_statement(bind_context, &Statement::Query(query.clone()))
                            .await
                    }
                    _ => {
                        return Err(ErrorCode::SyntaxException(
                            "COPY INTO <location> FROM <non-query> is invalid",
                        ));
                    }
                }
            }
            CopyIntoLocationSource::Query(query) => {
                self.init_cte(bind_context, &stmt.with)?;
                self.bind_statement(bind_context, &Statement::Query(query.clone()))
                    .await
            }
        }?;
        let mut is_ordered = false;
        if let Plan::Query { s_expr, .. } = &query {
            let p = s_expr.derive_relational_prop()?;
            if !p.orderings.is_empty() {
                is_ordered = true;
            }
        }

        let (mut stage_info, path) = resolve_file_location(self.ctx.as_ref(), &stmt.dst).await?;

        if stmt.options.use_raw_path {
            if path.ends_with("/") {
                return Err(ErrorCode::BadArguments(
                    "when use_raw_path is set to true, url path can not end with '/'",
                ));
            }

            let op = init_stage_operator(&stage_info)?;
            if !stmt.options.overwrite {
                match op.stat(&path).await {
                    Ok(_) => return Err(ErrorCode::BadArguments("file already exists")),
                    Err(e) => {
                        if e.kind() != ErrorKind::NotFound {
                            return Err(e.into());
                        }
                    }
                }
            }
        }

        if !stmt.file_format.is_empty() {
            stage_info.file_format_params = self.try_resolve_file_format(&stmt.file_format).await?;
        }
        let info = CopyIntoLocationInfo {
            stage: Box::new(stage_info),
            path,
            options: stmt.options.clone(),
            is_ordered,
        };
        Ok(Plan::CopyIntoLocation(CopyIntoLocationPlan {
            from: Box::new(query),
            info,
        }))
    }
}
