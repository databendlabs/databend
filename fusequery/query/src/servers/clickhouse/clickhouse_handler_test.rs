// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::{Result, ErrorCode};
use crate::servers::ClickHouseHandler;
use crate::sessions::SessionManager;
use common_runtime::tokio;
use clickhouse_rs::{Pool, ClientHandle, Block};
use clickhouse_rs::types::Complex;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_clickhouse_handler_query() -> Result<()> {
    let sessions = SessionManager::try_create(1)?;
    let handler = ClickHouseHandler::create(sessions);

    let listening = handler.start(("0.0.0.0".to_string(), 0_u16)).await?;
    let mut handler = create_conn(listening.port()).await?;

    let query_str = "SELECT COUNT() AS c FROM numbers(1000)";
    let block = query(&mut handler, query_str).await?;
    assert_eq!(block.row_count(), 1);
    assert_eq!(get_u64_data(block)?, Some(1000));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_reject_clickhouse_connection() -> Result<()> {
    let sessions = SessionManager::try_create(1)?;
    let handler = ClickHouseHandler::create(sessions);

    let listening = handler.start(("0.0.0.0".to_string(), 0_u16)).await?;
    {
        // Accepted connection
        let _handler = create_conn(listening.port()).await?;

        // Rejected connection
        match create_conn(listening.port()).await {
            Ok(_) => assert!(false, "Create clickhouse connection must be reject."),
            Err(error) => {
                let message = error.message();
                assert!(message.find("NO_FREE_CONNECTION").is_some());
            }
        }
    }

    // Wait for the connection to be destroyed
    std::thread::sleep(Duration::from_secs(5));
    // Accepted connection
    create_conn(listening.port()).await?;

    Ok(())
}

fn get_u64_data(block: Block<Complex>) -> Result<Option<u64>> {
    match block.get(0, "c") {
        Ok(value) => Ok(value),
        Err(error) => {
            Err(ErrorCode::UnknownException(format!(
                "Cannot get data {:?}", error
            )))
        }
    }
}

async fn query(handler: &mut ClientHandle, query: &str) -> Result<Block<Complex>> {
    let query_result = handler.query(query);
    match query_result.fetch_all().await {
        Ok(block) => Ok(block),
        Err(error) => {
            Err(ErrorCode::UnknownException(format!(
                "Error query: {:?}", error
            )))
        }
    }
}

async fn create_conn(port: u16) -> Result<ClientHandle> {
    let url = format!("tcp://default:@127.0.0.1:{}/default?compression=lz4&ping_timeout=10s&connection_timeout=20s", port);
    let get_handle = Pool::new(url).get_handle();
    match get_handle.await {
        Ok(client_handle) => Ok(client_handle),
        Err(error) => {
            Err(ErrorCode::UnknownException(format!(
                "Reject connection, cause:{:?}", error
            )))
        }
    }
}