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

use databend_common_ast::ast::Query;
use databend_common_ast::ast::SetExpr;

use crate::Runner;

impl Runner {
    pub(crate) async fn try_reduce_query(&mut self, code: u64, mut query: Query) -> Query {
        // reduce limit and offset
        if !query.limit.is_empty() || query.offset.is_some() {
            let mut reduced_query = query.clone();
            reduced_query.limit = vec![];
            reduced_query.offset = None;
            if self.execute_reduce_query(code, &reduced_query).await {
                query = reduced_query;
            }
        }

        // reduce order by
        if !query.order_by.is_empty() {
            let mut reduced_query = query.clone();
            reduced_query.order_by = vec![];
            if self.execute_reduce_query(code, &reduced_query).await {
                query = reduced_query;
            }
        }

        if let SetExpr::Select(select_stmt) = query.body.clone() {
            let mut select_stmt = select_stmt.clone();
            let mut reduced_query = query.clone();
            // TODO: reduce expr
            if select_stmt.selection.is_some() {
                let mut reduced_select_stmt = select_stmt.clone();
                reduced_select_stmt.selection = None;
                reduced_query.body = SetExpr::Select(reduced_select_stmt.clone());
                if self.execute_reduce_query(code, &reduced_query).await {
                    select_stmt = reduced_select_stmt;
                }
            }
            if select_stmt.having.is_some() {
                let mut reduced_select_stmt = select_stmt.clone();
                reduced_select_stmt.having = None;
                reduced_query.body = SetExpr::Select(reduced_select_stmt.clone());
                if self.execute_reduce_query(code, &reduced_query).await {
                    select_stmt = reduced_select_stmt;
                }
            }
            if select_stmt.window_list.is_none() && reduced_query.with.is_some() {
                reduced_query.with = None;
                if self.execute_reduce_query(code, &reduced_query).await {
                    query = reduced_query.clone();
                }
            }
            let select_list = select_stmt.select_list.clone();
            let mut reduced_select_stmt = select_stmt.clone();
            for item in &select_list {
                reduced_select_stmt.select_list = vec![item.clone()];
                reduced_query.body = SetExpr::Select(reduced_select_stmt.clone());
                if self.execute_reduce_query(code, &reduced_query).await {
                    select_stmt = reduced_select_stmt;
                    break;
                }
            }
            query.body = SetExpr::Select(select_stmt);
        }

        query
    }

    async fn execute_reduce_query(&mut self, err_code: u64, query: &Query) -> bool {
        let query_sql = query.to_string();
        if let Ok(responses) = self.client.query(&query_sql).await {
            if let Some(error) = &responses[0].error {
                let value = error.as_object().unwrap();
                let code = value["code"].as_u64().unwrap();
                return err_code == code;
            }
        }
        false
    }
}
