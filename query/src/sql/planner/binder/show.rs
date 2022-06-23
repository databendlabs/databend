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

use common_ast::ast::ShowLimit;
use common_ast::parser::error::Backtrace;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_exception::Result;

use crate::sql::plans::Plan;
use crate::sql::BindContext;
use crate::sql::Binder;

impl<'a> Binder {
    pub(super) async fn bind_show_functions(
        &mut self,
        bind_context: &BindContext,
        limit: &Option<ShowLimit<'a>>,
    ) -> Result<Plan> {
        // rewrite show functions to select * from system.functions ...
        let query = format!("SELECT name, is_builtin, is_aggregate, definition, description FROM system.functions {} ORDER BY name",
            match limit {
                None => {
                    "".to_string()
                }
                Some(predicate) => {
                    match predicate {
                        ShowLimit::Like { pattern } => {
                            format!("WHERE name LIKE '{}'", pattern)
                        }
                        ShowLimit::Where { selection } => {
                            format!("WHERE {}", selection)
                        }
                    }
                }
            }
        );
        let tokens = tokenize_sql(query.as_str())?;
        let backtrace = Backtrace::new();
        let stmt = parse_sql(&tokens, &backtrace)?;
        self.bind_statement(bind_context, &stmt).await
    }
}
