// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::{Result, ErrorCodes, ToErrorCodes};
use crate::servers::{MySQLHandler, RunnableServer};
use crate::configs::Config;
use crate::clusters::Cluster;
use crate::sessions::SessionManager;
use mysql::{Error, Conn, FromRowError, Row};
use mysql::prelude::Queryable;
use mysql::prelude::FromRow;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_use_database_with_on_query() -> Result<()> {
    let handler = MySQLHandler::create(
        Config::default(),
        Cluster::empty(),
        SessionManager::create(1),
    );

    let runnable_server = handler.start("0.0.0.0", 0).await?;
    let mut connection = create_connection(runnable_server.listener_address().port())?;
    let received_data: Vec<(String)> = query(&mut connection, "SELECT database()")?;
    assert_eq!(received_data, vec!["default"]);
    query::<EmptyRow>(&mut connection, "USE system")?;
    let received_data: Vec<(String)> = query(&mut connection, "SELECT database()")?;
    assert_eq!(received_data, vec!["system"]);

    Ok(())
}

fn query<T: FromRow>(connection: &mut Conn, query: &str) -> Result<Vec<T>> {
    connection.query::<T, &str>(query).map_err_to_code(ErrorCodes::UnknownException, || "")
}

fn create_connection(port: u16) -> Result<mysql::Conn> {
    let uri = &format!("mysql://127.0.0.1:{}", port);
    mysql::Conn::new(uri)
        .map_err_to_code(ErrorCodes::UnknownException, || "")
}

struct EmptyRow;

impl FromRow for EmptyRow {
    fn from_row_opt(_: Row) -> std::result::Result<Self, FromRowError>
        where Self: Sized {
        Ok(EmptyRow)
    }
}
