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

use databend_common_ast::ast::CallStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::Binder;
use crate::plans::Plan;
use crate::BindContext;

impl Binder {
    /// Rewrite call stmt to table functions
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_call(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CallStmt,
    ) -> Result<Plan> {
        let table_function_name = stmt.name.split('$').last().unwrap();

        let query = if table_function_name.eq_ignore_ascii_case("search_tables") {
            if stmt.args.len() != 1 {
                return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "Incorrect number of arguments to function {}. Expected 1, got {}",
                    stmt.name,
                    stmt.args.len()
                )));
            }
            format!(
                "SELECT * FROM system.tables WHERE name like '%{}%' ORDER BY database, name",
                stmt.args[0]
            )
        } else {
            format!(
                "SELECT * from {table_function_name}({})",
                stmt.args
                    .iter()
                    .map(|x| format!("'{}'", x))
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        };

        self.bind_rewrite_to_query(bind_context, &query, crate::plans::RewriteKind::Call)
            .await
    }
}
