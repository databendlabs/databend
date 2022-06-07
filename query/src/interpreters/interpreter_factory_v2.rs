// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;

use super::CreateDatabaseInterpreter;
use super::CreateTableInterpreter;
use super::CreateViewInterpreter;
use super::ExplainInterpreterV2;
use super::InterpreterPtr;
use super::SelectInterpreterV2;
use super::ShowMetricsInterpreter;
use super::ShowProcessListInterpreter;
use super::ShowSettingsInterpreter;
use crate::interpreters::AlterUserInterpreter;
use crate::interpreters::CreateUserInterpreter;
use crate::interpreters::DropUserInterpreter;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;
use crate::sql::DfStatement;

/// InterpreterFactory is the entry of Interpreter.
pub struct InterpreterFactoryV2;

/// InterpreterFactoryV2 provides `get` method which transforms `Plan` into the corresponding interpreter.
/// Such as: Plan::Query -> InterpreterSelectV2
impl InterpreterFactoryV2 {
    /// Check if statement is supported by InterpreterFactoryV2
    pub fn check(stmt: &DfStatement) -> bool {
        matches!(
            stmt,
            DfStatement::Query(_)
                | DfStatement::Explain(_)
                | DfStatement::CreateTable(_)
                | DfStatement::CreateView(_)
                | DfStatement::ShowMetrics(_)
                | DfStatement::ShowProcessList(_)
                | DfStatement::ShowSettings(_)
                | DfStatement::CreateDatabase(_)
        )
    }

    pub fn get(ctx: Arc<QueryContext>, plan: &Plan) -> Result<InterpreterPtr> {
        let inner = match plan {
            Plan::Query {
                s_expr,
                bind_context,
                metadata,
            } => SelectInterpreterV2::try_create(
                ctx,
                *bind_context.clone(),
                s_expr.clone(),
                metadata.clone(),
            ),
            Plan::Explain { kind, plan } => {
                ExplainInterpreterV2::try_create(ctx, *plan.clone(), kind.clone())
            }
            Plan::CreateTable(create_table) => {
                CreateTableInterpreter::try_create(ctx, *create_table.clone())
            }
            Plan::CreateDatabase(create_database) => {
                CreateDatabaseInterpreter::try_create(ctx, create_database.clone())
            }
            Plan::ShowMetrics => ShowMetricsInterpreter::try_create(ctx),
            Plan::ShowProcessList => ShowProcessListInterpreter::try_create(ctx),
            Plan::ShowSettings => ShowSettingsInterpreter::try_create(ctx),
            Plan::AlterUser(alter_user) => {
                AlterUserInterpreter::try_create(ctx, *alter_user.clone())
            }
            Plan::CreateUser(create_user) => {
                CreateUserInterpreter::try_create(ctx, *create_user.clone())
            }
            Plan::CreateView(create_view) => {
                CreateViewInterpreter::try_create(ctx, *create_view.clone())
            }
            Plan::DropUser(drop_user) => DropUserInterpreter::try_create(ctx, *drop_user.clone()),
        }?;
        Ok(inner)
    }
}
