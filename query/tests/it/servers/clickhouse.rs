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

use std::net::SocketAddr;
use std::time::Duration;

use clickhouse_driver::prelude::*;
use common_base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::servers::ClickHouseHandler;
use databend_query::servers::Server;
use tempfile::TempDir;
use uuid::Uuid;

use crate::tests::SessionManagerBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_clickhouse_handler_query() -> Result<()> {
    struct Sum {
        sum: u64,
    }
    impl clickhouse_driver::prelude::Deserialize for Sum {
        fn deserialize(row: Row) -> errors::Result<Self> {
            let sum = row.value(0).unwrap().unwrap();
            Ok(Sum { sum })
        }
    }
    let (_, listening) = start_server(1).await?;
    let mut conn = create_conn(listening.port()).await?;
    let query_str = "SELECT COUNT() AS c FROM numbers(1000)";
    let block = query::<Sum>(&mut conn, query_str).await?;
    assert_eq!(block.len(), 1);
    assert_eq!(block[0].sum, 1000);
    Ok(())
}

#[derive(Debug)]
struct Temp {
    a: u64,
    b: i64,
    c: String,
}

impl clickhouse_driver::prelude::Deserialize for Temp {
    fn deserialize(row: Row) -> errors::Result<Self> {
        let a = row.value(0).unwrap().unwrap();
        let b = row.value(1).unwrap().unwrap();
        let c: &str = row.value(2).unwrap().unwrap();
        Ok(Temp {
            a,
            b,
            c: c.to_string(),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_clickhouse_insert_data() -> Result<()> {
    let (_, listening) = start_server(1).await?;
    let mut conn = create_conn(listening.port()).await?;

    let query_str =
        "CREATE TABLE test(a UInt64 not null, b Int64 not null, c String not null) Engine = Memory";
    execute(&mut conn, query_str).await?;

    let block = Block::new("test")
        .add("a", vec![3u64, 4, 5, 6])
        .add("b", vec![33i64, 44, 55, 66])
        .add("c", vec!["33", "44", "55", "66"]);

    insert(&mut conn, &block).await?;

    let query_str = "SELECT * from test order by a";
    let datas = query::<Temp>(&mut conn, query_str).await?;
    assert_eq!(datas.len(), 4);
    assert_eq!(datas[0].a, 3);
    assert_eq!(datas[0].b, 33);
    assert_eq!(datas[0].c, "33".to_string());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore]
async fn test_clickhouse_insert_to_fuse_table() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let data_path = tmp_dir.path().to_str().unwrap().to_string();
    let mut handler = ClickHouseHandler::create(
        SessionManagerBuilder::create()
            .max_sessions(1)
            .disk_storage_path(data_path)
            .build()?,
    );

    let listening = "127.0.0.1:0".parse::<SocketAddr>()?;
    let listening = handler.start(listening).await?;

    let mut conn = create_conn(listening.port()).await?;

    let test_tbl_name = format!("tbl_{}", Uuid::new_v4().simple());
    let query_str = format!(
        "CREATE TABLE {}(a UInt64 not null, b Int64 not null, c String not null) Engine = fuse",
        test_tbl_name
    );
    execute(&mut conn, &query_str).await?;

    let block = Block::new(test_tbl_name.as_str())
        .add("a", vec![3u64, 4, 5, 6])
        .add("b", vec![33i64, 44, 55, 66])
        .add("c", vec!["33", "44", "55", "66"]);

    insert(&mut conn, &block).await?;
    // we give another insertion here, to test if everything still doing well
    // see issue #2460 https://github.com/datafuselabs/databend/issues/2460
    insert(&mut conn, &block).await?;

    let query_str = format!("SELECT * from {}", test_tbl_name);
    let data = query::<Temp>(&mut conn, &query_str).await?;
    assert_eq!(data.len(), 8);

    // insert and select
    let insert_str = format!(
        "insert into {} select * from {}",
        test_tbl_name, test_tbl_name
    );

    // ignore the error here, because query needs a block to return but here can't
    let _ = query::<Temp>(&mut conn, &insert_str).await;
    let data = query::<Temp>(&mut conn, &query_str).await?;
    assert_eq!(data.len(), 16);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_reject_clickhouse_connection() -> Result<()> {
    let (_, listening) = start_server(1).await?;

    {
        // Accepted connection
        let _conn = create_conn(listening.port()).await?;

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
    std::thread::sleep(Duration::from_secs(1));
    // Accepted connection
    create_conn(listening.port()).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_abort_clickhouse_server() -> Result<()> {
    let (mut handler, listening) = start_server(1).await?;
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

async fn start_server(max_sessions: u64) -> Result<(Box<dyn Server>, SocketAddr)> {
    let mut handler = ClickHouseHandler::create(
        SessionManagerBuilder::create()
            .max_sessions(max_sessions)
            .build()?,
    );

    let listening = "127.0.0.1:0".parse::<SocketAddr>()?;
    let listening = handler.start(listening).await?;
    Ok((handler, listening))
}

async fn query<T>(client: &mut Connection, query: &str) -> Result<Vec<T>>
where T: clickhouse_driver::prelude::Deserialize {
    let mut result = client.query(query).await.unwrap();
    let mut results = vec![];
    while let Some(block) = result.next().await.unwrap() {
        for item in block.iter::<T>() {
            results.push(item);
        }
    }
    Ok(results)
}

async fn execute(client: &mut Connection, query: &str) -> Result<()> {
    match client.execute(query).await {
        Ok(_) => Ok(()),
        Err(error) => Err(ErrorCode::UnknownException(format!(
            "Error execute query: {:?}",
            error
        ))),
    }
}

//block contains table name
async fn insert<'a>(client: &mut Connection, block: &Block<'a>) -> Result<()> {
    match client.insert(block).await {
        Ok(mut isrt) => isrt
            .commit()
            .await
            .map_err(|err| ErrorCode::UnknownException(err.to_string())),
        Err(error) => Err(ErrorCode::UnknownException(format!(
            "Error insert query: {:?}",
            error
        ))),
    }
}

async fn create_conn(port: u16) -> Result<Connection> {
    let url = format!("tcp://default:@127.0.0.1:{}/default?compression=lz4&ping_timeout=10s&connection_timeout=20s", port);
    let pool = Pool::create(url).map_err(|err| ErrorCode::UnknownException(err.to_string()))?;
    let c = pool.connection().await;

    match c {
        Ok(c) => Ok(c),
        Err(error) => Err(ErrorCode::UnknownException(format!(
            "Error connect to clickhouse: {:?}",
            error
        ))),
    }
}
