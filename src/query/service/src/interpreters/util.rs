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

use databend_common_ast::ast::Expr;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::SENSITIVE_SYSTEM_RESOURCE;
use databend_common_script::ir::ColumnAccess;
use databend_common_script::Client;
use databend_common_sql::Planner;
use futures_util::TryStreamExt;
use itertools::Itertools;

use crate::interpreters::interpreter::auto_commit_if_not_allowed_in_transaction;
use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;

pub fn check_system_history(
    catalog: &Arc<dyn Catalog>,
    db_name: &str,
) -> databend_common_exception::Result<()> {
    if !catalog.is_external() && db_name.eq_ignore_ascii_case(SENSITIVE_SYSTEM_RESOURCE) {
        return Err(ErrorCode::IllegalGrant(
            "Can not modify system_history ownership",
        ));
    }
    Ok(())
}

pub fn check_system_history_stage(
    stage_name: &str,
    sensitive_system_stage: &str,
) -> databend_common_exception::Result<()> {
    if stage_name.eq_ignore_ascii_case(sensitive_system_stage) {
        return Err(ErrorCode::IllegalGrant(
            "Can not modify system history stage {sensitive_system_stage} ownership",
        ));
    }
    Ok(())
}

#[allow(clippy::type_complexity)]
pub fn generate_desc_schema(
    schema: TableSchemaRef,
) -> (
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
) {
    let mut names: Vec<String> = vec![];
    let mut types: Vec<String> = vec![];
    let mut nulls: Vec<String> = vec![];
    let mut default_exprs: Vec<String> = vec![];
    let mut extras: Vec<String> = vec![];

    for field in schema.fields().iter() {
        names.push(field.name().to_string());

        let non_null_type = field.data_type().remove_recursive_nullable();
        types.push(non_null_type.sql_name());
        nulls.push(if field.is_nullable() {
            "YES".to_string()
        } else {
            "NO".to_string()
        });
        match field.default_expr() {
            Some(expr) => {
                default_exprs.push(expr.clone());
            }

            None => {
                let value = Scalar::default_value(&field.data_type().into());
                default_exprs.push(value.to_string());
            }
        }
        let extra = match field.computed_expr() {
            Some(ComputedExpr::Virtual(expr)) => format!("VIRTUAL COMPUTED COLUMN `{}`", expr),
            Some(ComputedExpr::Stored(expr)) => format!("STORED COMPUTED COLUMN `{}`", expr),
            _ => "".to_string(),
        };
        extras.push(extra);
    }
    (names, types, nulls, default_exprs, extras)
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub(crate) schema: DataSchemaRef,
    pub(crate) block: DataBlock,
}

pub struct ScriptClient {
    pub(crate) ctx: Arc<QueryContext>,
}

impl Client for ScriptClient {
    type Var = Scalar;
    type Set = QueryResult;

    async fn query(&self, query: &str) -> databend_common_exception::Result<Self::Set> {
        let ctx = self
            .ctx
            .get_current_session()
            .create_query_context(&databend_common_version::BUILD_INFO)
            .await?;

        let mut planner = Planner::new(ctx.clone());
        // In script ignore query level settings.
        let extras = planner.parse_sql(query)?;
        auto_commit_if_not_allowed_in_transaction(ctx.clone(), &extras.statement).await?;
        let plan = planner.plan_stmt(&extras.statement, false).await?;

        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = interpreter.execute(ctx.clone()).await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let mut schema = plan.schema();
        if let Some(real_schema) = interpreter.get_dynamic_schema().await {
            schema = real_schema;
        }

        let block = match blocks.len() {
            0 => DataBlock::empty_with_schema(schema.clone()),
            1 => blocks[0].clone(),
            _ => DataBlock::concat(&blocks)?,
        };

        Ok(QueryResult { schema, block })
    }

    fn var_to_ast(&self, scalar: &Self::Var) -> databend_common_exception::Result<Expr> {
        let scalar = scalar.to_string();
        let ast = parse_expr(&tokenize_sql(&scalar)?, Dialect::PostgreSQL)?;

        Ok(ast)
    }

    fn read_from_set(
        &self,
        set: &Self::Set,
        row: usize,
        col: &ColumnAccess,
    ) -> databend_common_exception::Result<Self::Var> {
        let offset = match col {
            ColumnAccess::Position(offset) => *offset,
            // TODO(andylokandy): name resolution
            ColumnAccess::Name(name) => set
                .schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                ErrorCode::ScriptExecutionError(format!(
                    "cannot find column with name {} in block, available columns: {}",
                    name,
                    set.schema
                        .fields()
                        .iter()
                        .map(|f| format!("'{}'", f.name()))
                        .join(", ")
                ))
            })?,
        };
        let col = set.block.columns().get(offset).ok_or_else(|| {
            ErrorCode::ScriptExecutionError(format!(
                "cannot read column at offset {} from block with {} columns",
                offset,
                set.block.num_columns()
            ))
        })?;
        let cell = col
            .index(row)
            .ok_or_else(|| {
                ErrorCode::ScriptExecutionError(format!(
                    "cannot read value at row {} from column with {} rows",
                    row,
                    set.block.num_rows(),
                ))
            })?
            .to_owned();

        Ok(cell)
    }

    fn num_rows(&self, set: &Self::Set) -> usize {
        set.block.num_rows()
    }

    fn is_true(&self, scalar: &Self::Var) -> databend_common_exception::Result<bool> {
        match scalar {
            Scalar::Boolean(v) => Ok(*v),
            _ => Err(ErrorCode::ScriptExecutionError(format!(
                "`is_true` called on non-boolean value {scalar}",
            ))),
        }
    }
}

#[derive(serde::Serialize)]
pub struct AuditElement<'a, T: serde::Serialize> {
    user: String,
    hostname: String,
    action_type: &'static str,
    action: &'a T,
}

impl<'a, T: serde::Serialize> AuditElement<'a, T> {
    pub fn create(user: &UserInfo, action_type: &'static str, action: &'a T) -> Self {
        AuditElement {
            action,
            action_type,
            user: user.name.clone(),
            hostname: user.hostname.clone(),
        }
    }
}
