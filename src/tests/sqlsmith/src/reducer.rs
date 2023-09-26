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

use common_ast::ast::Query;
use common_ast::ast::SetExpr;
use databend_client::error::Error as ClientError;
use databend_driver::Connection;
use databend_driver::Error;

pub struct Reducer {
    pub origin_query: Query,
    origin_code: u16,
}

impl Reducer {
    pub(crate) fn new(origin_query: Query, origin_code: u16) -> Reducer {
        Reducer {
            origin_query,
            origin_code,
        }
    }

    pub async fn try_reduce_query(&self, conn: Box<dyn Connection>, query: &Query) {
        let mut query = query.clone();
        // reduce limit and offset
        if !query.limit.is_empty() || query.offset.is_some() {
            let mut reduce_query = query.clone();
            reduce_query.limit = vec![];
            if let Some(diff_code) = self.execute_reduce_query(conn.clone(), &reduce_query).await {
                if diff_code {
                    tracing::info!(
                        "query_sql: \x1b[0;32m{:?}\x1b[0m",
                        self.origin_query.to_string()
                    );
                    return;
                }
            }
        }

        // reduce order by
        if !query.order_by.is_empty() {
            let mut reduce_query = query.clone();
            reduce_query.order_by = vec![];
            if let Some(diff_code) = self.execute_reduce_query(conn.clone(), &reduce_query).await {
                if diff_code {
                    tracing::info!(
                        "query_sql: \x1b[0;32m{:?}\x1b[0m",
                        self.origin_query.to_string()
                    );
                    return;
                }
            }
        }

        if let SetExpr::Select(select_stmt) = query.body.clone() {
            let having = select_stmt.having.clone();
            let selection = select_stmt.selection.clone();

            let mut reduced_select_list = vec![];
            let mut new_select_list = select_stmt.select_list.clone();
            if new_select_list.len() != 1 {
                // reduce select list
                let mut may_err_select_list = vec![];
                while let Some(last) = new_select_list.pop() {
                    let mut select_stmt = select_stmt.clone();
                    select_stmt.select_list = new_select_list.clone();
                    let body = SetExpr::Select(select_stmt);
                    let mut reduce_query = query.clone();
                    reduce_query.body = body;
                    if let Some(diff_code) =
                        self.execute_reduce_query(conn.clone(), &reduce_query).await
                    {
                        if diff_code && !new_select_list.is_empty() {
                            tracing::info!(
                                "query_sql: \x1b[0;32m{:?}\x1b[0m",
                                self.origin_query.to_string()
                            );
                            return;
                        }
                    } else {
                        may_err_select_list.push(last);
                    }
                }

                if new_select_list.is_empty() {
                    let first_target = select_stmt.select_list.first().unwrap().clone();
                    reduced_select_list.push(first_target);
                }
                let select_list = if !may_err_select_list.is_empty() {
                    may_err_select_list
                } else if !reduced_select_list.is_empty() {
                    reduced_select_list.clone()
                } else {
                    select_stmt.select_list.clone()
                };
                let mut select_stmt = select_stmt.clone();
                select_stmt.select_list = select_list;
                let body = SetExpr::Select(select_stmt);
                let mut reduce_query = query.clone();
                reduce_query.body = body;
                query = reduce_query;
            }
            let new_select_list = if reduced_select_list.is_empty() {
                select_stmt.select_list.clone()
            } else {
                reduced_select_list
            };
            // reduce having
            if having.is_some() {
                let mut select_stmt = select_stmt.clone();
                select_stmt.select_list = new_select_list.clone();
                select_stmt.having = None;
                let body = SetExpr::Select(select_stmt);
                let mut reduce_query = query.clone();
                reduce_query.body = body;
                if let Some(diff_code) =
                    self.execute_reduce_query(conn.clone(), &reduce_query).await
                {
                    if diff_code {
                        tracing::info!(
                            "query_sql: \x1b[0;32m{:?}\x1b[0m",
                            self.origin_query.to_string()
                        );
                        return;
                    }
                }
            }
            // reduce selection
            if selection.is_some() {
                let mut select_stmt = select_stmt.clone();
                select_stmt.select_list = new_select_list;
                select_stmt.selection = None;
                let body = SetExpr::Select(select_stmt);
                let mut reduce_query = query.clone();
                reduce_query.body = body;
                if let Some(diff_code) =
                    self.execute_reduce_query(conn.clone(), &reduce_query).await
                {
                    if diff_code {
                        tracing::info!(
                            "query_sql: \x1b[0;32m{:?}\x1b[0m",
                            self.origin_query.to_string()
                        );
                        return;
                    }
                }
            }
        }

        tracing::info!("query_sql: \x1b[0;32m{:?}\x1b[0m", query.to_string());
    }

    pub async fn execute_reduce_query(
        &self,
        conn: Box<dyn Connection>,
        query: &Query,
    ) -> Option<bool> {
        let sql = query.to_string();
        if let Err(Error::Api(ClientError::InvalidResponse(err))) = conn.exec(&sql).await {
            return Some(err.code != self.origin_code);
        }
        None
    }
}
