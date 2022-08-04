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
use common_exception::Result;

use crate::sql::plans::Plan;
use crate::sql::plans::RewriteKind;
use crate::sql::BindContext;
use crate::sql::Binder;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_show_functions(
        &mut self,
        bind_context: &BindContext,
        limit: &Option<ShowLimit<'a>>,
    ) -> Result<Plan> {
        // rewrite show functions to select * from system.functions ...
        let query = format!(
            "SELECT name, is_builtin, is_aggregate, definition, description FROM system.functions {} ORDER BY name",
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
        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowFunctions)
            .await
    }

    pub(in crate::sql::planner::binder) async fn bind_show_settings(
        &mut self,
        bind_context: &BindContext,
        like: &Option<String>,
    ) -> Result<Plan> {
        let sub_query = like
            .clone()
            .map(|s| format!("WHERE name LIKE '{s}'"))
            .unwrap_or_else(|| "".to_string());
        let query = format!(
            "SELECT name, value, default, level, description, type FROM system.settings {} ORDER BY name",
            sub_query
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowSettings)
            .await
    }
}
