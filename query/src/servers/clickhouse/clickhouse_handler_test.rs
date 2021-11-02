// Copyright 2020 Datafuse Labs.
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

use std::net::SocketAddr;
use std::time::Duration;

use clickhouse_rs::types::Complex;
use clickhouse_rs::Block;
use clickhouse_rs::ClientHandle;
use clickhouse_rs::Pool;
use common_base::tokio;
use common_base::uuid::Uuid;
use common_exception::ErrorCode;
use common_exception::Result;
use tempfile::TempDir;

use crate::servers::ClickHouseHandler;
use crate::tests::SessionManagerBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_clickhouse_handler_query() -> Result<()> {
    let mut handler =
        ClickHouseHandler::create(SessionManagerBuilder::create().max_sessions(1).build()?);

    let listening = "0.0.0.0:0".parse::<SocketAddr>()?;
    let listening = handler.start(listening).await?;
    let mut handler = create_conn(listening.port()).await?;

    let query_str = "SELECT COUNT() AS c FROM numbers(1000)";
    let block = query(&mut handler, query_str).await?;
    assert_eq!(block.row_count(), 1);
    assert_eq!(get_u64_data(block)?, 1000);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_clickhouse_insert_data() -> Result<()> {
    let mut handler =
        ClickHouseHandler::create(SessionManagerBuilder::create().max_sessions(1).build()?);

    let listening = "0.0.0.0:0".parse::<SocketAddr>()?;
    let listening = handler.start(listening).await?;
    let mut handler = create_conn(listening.port()).await?;

    let query_str = "CREATE TABLE test(a UInt64, b String) Engine = Memory";
    execute(&mut handler, query_str).await?;

    let block = Block::new();
    let block = block.column("a", vec![3u64, 4, 5, 6]);
    let block = block.column("b", vec!["33", "44", "55", "66"]);
    insert(&mut handler, "test", block).await?;

    let query_str = "SELECT * from test";
    let block = query(&mut handler, query_str).await?;
    assert_eq!(block.row_count(), 4);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_clickhouse_insert_to_fuse_table() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let data_path = tmp_dir.path().to_str().unwrap().to_string();
    let mut handler = ClickHouseHandler::create(
        SessionManagerBuilder::create()
            .max_sessions(1)
            .disk_storage_path(data_path)
            .build()?,
    );

    let listening = "0.0.0.0:0".parse::<SocketAddr>()?;
    let listening = handler.start(listening).await?;
    let mut handler = create_conn(listening.port()).await?;

    let test_tbl_name = format!("tbl_{}", Uuid::new_v4().to_simple().to_string());
    let query_str = format!(
        "CREATE TABLE {}(a UInt32, b UInt64, c String) Engine = fuse",
        test_tbl_name
    );
    execute(&mut handler, &query_str).await?;

    let block = Block::new();
    let block = block.column("a", vec![1u32, 2]);
    let block = block.column("b", vec![1u64, 2]);
    let block = block.column("c", vec!["1", "'\"2\"-\"2\"'"]);
    insert(&mut handler, &test_tbl_name, block.clone()).await?;
    // we give another insertion here, to test if everything still doing well
    // see issue #2460 https://github.com/datafuselabs/databend/issues/2460
    insert(&mut handler, &test_tbl_name, block).await?;

    let query_str = format!("SELECT * from {}", test_tbl_name);
    let block = query(&mut handler, &query_str).await?;
    assert_eq!(block.row_count(), 4);
    assert_eq!(block.column_count(), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_reject_clickhouse_connection() -> Result<()> {
    let mut handler =
        ClickHouseHandler::create(SessionManagerBuilder::create().max_sessions(1).build()?);

    let listening = "0.0.0.0:0".parse::<SocketAddr>()?;
    let listening = handler.start(listening).await?;
    {
        // Accepted connection
        let _handler = create_conn(listening.port()).await?;

        // Rejected connection
        match create_conn(listening.port()).await {
            Ok(_) => panic!("Create clickhouse connection must be reject."),
            Err(error) => {
                let message = error.message();
                assert!(message.contains("NO_FREE_CONNECTION"));
            }
        }
    }

    // Wait for the connection to be destroyed
    std::thread::sleep(Duration::from_secs(5));
    // Accepted connection
    create_conn(listening.port()).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_abort_clickhouse_server() -> Result<()> {
    let mut handler =
        ClickHouseHandler::create(SessionManagerBuilder::create().max_sessions(3).build()?);

    let listening = "0.0.0.0:0".parse::<SocketAddr>()?;
    let listening = handler.start(listening).await?;

    // Accepted connection
    let _handler = create_conn(listening.port()).await?;

    handler.shutdown(true).await;

    // Rejected connection
    match create_conn(listening.port()).await {
        Ok(_) => panic!("Create clickhouse connection must be reject."),
        Err(error) => {
            let message = error.message();
            assert!(message.contains("ConnectionRefused"));
        }
    }

    Ok(())
}

fn get_u64_data(block: Block<Complex>) -> Result<u64> {
    match block.get(0, "c") {
        Ok(value) => Ok(value),
        Err(error) => Err(ErrorCode::UnknownException(format!(
            "Cannot get data {:?}",
            error
        ))),
    }
}

async fn query(handler: &mut ClientHandle, query: &str) -> Result<Block<Complex>> {
    let query_result = handler.query(query);
    match query_result.fetch_all().await {
        Ok(block) => Ok(block),
        Err(error) => Err(ErrorCode::UnknownException(format!(
            "Error query: {:?}",
            error
        ))),
    }
}

async fn execute(handler: &mut ClientHandle, query: &str) -> Result<()> {
    match handler.execute(query).await {
        Ok(()) => Ok(()),
        Err(error) => Err(ErrorCode::UnknownException(format!(
            "Error execute query: {:?}",
            error
        ))),
    }
}

async fn insert(handler: &mut ClientHandle, table: &str, block: Block) -> Result<()> {
    match handler.insert(table, block).await {
        Ok(()) => Ok(()),
        Err(error) => Err(ErrorCode::UnknownException(format!(
            "Error insert table: {:?}",
            error
        ))),
    }
}

async fn create_conn(port: u16) -> Result<ClientHandle> {
    let url = format!("tcp://default:@127.0.0.1:{}/default?compression=lz4&ping_timeout=10s&connection_timeout=20s", port);
    let get_handle = Pool::new(url).get_handle();
    match get_handle.await {
        Ok(client_handle) => Ok(client_handle),
        Err(error) => Err(ErrorCode::UnknownException(format!(
            "Reject connection, cause:{:?}",
            error
        ))),
    }
}
