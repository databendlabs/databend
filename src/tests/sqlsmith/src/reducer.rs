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

use databend_client::error::Error as ClientError;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SetExpr;
use databend_driver::Connection;
use databend_driver::Error;

pub(crate) async fn try_reduce_query(
    conn: Box<dyn Connection>,
    code: u16,
    mut query: Query,
) -> Query {
    // reduce limit and offset
    if !query.limit.is_empty() || query.offset.is_some() {
        let mut reduced_query = query.clone();
        reduced_query.limit = vec![];
        reduced_query.offset = None;
        if execute_reduce_query(conn.clone(), code, &reduced_query).await {
            query = reduced_query;
        }
    }

    // reduce order by
    if !query.order_by.is_empty() {
        let mut reduced_query = query.clone();
        reduced_query.order_by = vec![];
        if execute_reduce_query(conn.clone(), code, &reduced_query).await {
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
            if execute_reduce_query(conn.clone(), code, &reduced_query).await {
                select_stmt = reduced_select_stmt;
            }
        }
        if select_stmt.having.is_some() {
            let mut reduced_select_stmt = select_stmt.clone();
            reduced_select_stmt.having = None;
            reduced_query.body = SetExpr::Select(reduced_select_stmt.clone());
            if execute_reduce_query(conn.clone(), code, &reduced_query).await {
                select_stmt = reduced_select_stmt;
            }
        }
        if select_stmt.window_list.is_none() && reduced_query.with.is_some() {
            reduced_query.with = None;
            if execute_reduce_query(conn.clone(), code, &reduced_query).await {
                query = reduced_query.clone();
            }
        }
        let select_list = select_stmt.select_list.clone();
        let mut reduced_select_stmt = select_stmt.clone();
        for item in &select_list {
            reduced_select_stmt.select_list = vec![item.clone()];
            reduced_query.body = SetExpr::Select(reduced_select_stmt.clone());
            if execute_reduce_query(conn.clone(), code, &reduced_query).await {
                select_stmt = reduced_select_stmt;
                break;
            }
        }
        query.body = SetExpr::Select(select_stmt);
    }

    query
}

async fn execute_reduce_query(conn: Box<dyn Connection>, code: u16, query: &Query) -> bool {
    let sql = query.to_string();
    if let Err(Error::Api(ClientError::InvalidResponse(err))) = conn.exec(&sql).await {
        return err.code == code;
    }
    false
}
