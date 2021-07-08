// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_runtime::tokio;
use mysql::prelude::Queryable;

use crate::configs::Config;
use crate::servers::mysql::mysql_session::Session;
use crate::sessions::ISession;
use crate::sessions::SessionMgr;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idle_state_wait_terminal_with_not_abort() -> Result<()> {
    let (_conn, session) = prepare_session_and_connect().await?;

    match session.wait_terminal(Some(Duration::from_secs(1))).await {
        Ok(_) => assert!(false, "wait_terminal must be return timeout."),
        Err(error) => {
            assert_eq!(error.code(), 40);
            assert_eq!(error.message(), "Session did not close in 1s");
        }
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idle_wait_terminal_after_not_force_abort() -> Result<()> {
    let instant = Instant::now();
    let (_conn, session) = prepare_session_and_connect().await?;

    session.abort(false)?;
    match session.wait_terminal(Some(Duration::from_secs(5))).await {
        Ok(_) => assert!(true),
        Err(error) => assert!(false, "{:?}", error),
    };
    assert!(instant.elapsed().lt(&Duration::from_secs(5)));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idle_wait_terminal_before_force_abort() -> Result<()> {
    let (_conn, session) = prepare_session_and_connect().await?;

    let wait_terminal_session = session.clone();
    let wait_terminal_session_join_handle = tokio::spawn(async move {
        match wait_terminal_session.wait_terminal(None).await {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, "wait_terminal must be return Ok. {}", error),
        };
    });

    session.abort(true)?;
    match wait_terminal_session_join_handle.await {
        Ok(_) => assert!(true),
        Err(err) => assert!(false, "wait_terminal error {}", err),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idle_wait_terminal_after_force_abort() -> Result<()> {
    let (_conn, session) = prepare_session_and_connect().await?;

    session.abort(true)?;
    match session.wait_terminal(None).await {
        Ok(_) => assert!(true),
        Err(error) => assert!(false, "wait_terminal must be return Ok. {}", error),
    };

    // test wait_terminal again.
    match session.wait_terminal(None).await {
        Ok(_) => assert!(true),
        Err(error) => assert!(false, "wait_terminal must be return Ok. {}", error),
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_progress_state_wait_terminal_with_not_abort() -> Result<()> {
    let instant = Instant::now();
    let (mut conn, session) = prepare_session_and_connect().await?;

    let query_join_handler = tokio::spawn(async move {
        conn.query::<Vec<u8>, &str>("SET max_threads = 1").unwrap();
        conn.query::<Vec<u8>, &str>("SET max_block_size = 1")
            .unwrap();

        match conn.query::<Vec<u8>, &str>("SELECT sleep(1) FROM numbers(15)") {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, "{:?}", error),
        };
    });

    // Wait for `SELECT sleep(5)` to become running
    std::thread::sleep(Duration::from_secs(5));

    match session.wait_terminal(Some(Duration::from_secs(1))).await {
        Ok(_) => assert!(false, "wait_terminal must be return timeout."),
        Err(error) => {
            assert_eq!(error.code(), 40);
            assert_eq!(error.message(), "Session did not close in 1s");
        }
    };

    assert!(instant.elapsed().gt(&Duration::from_secs(6)));
    assert!(instant.elapsed().lt(&Duration::from_secs(10)));
    query_join_handler.await.unwrap();
    assert!(instant.elapsed().gt(&Duration::from_secs(15)));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_progress_wait_terminal_after_not_force_abort() -> Result<()> {
    let instant = Instant::now();
    let (mut conn, session) = prepare_session_and_connect().await?;

    let query_join_handler = tokio::spawn(async move {
        conn.query::<Vec<u8>, &str>("SET max_threads = 1").unwrap();
        conn.query::<Vec<u8>, &str>("SET max_block_size = 1")
            .unwrap();

        match conn.query::<Vec<u8>, &str>("SELECT sleep(1) FROM numbers(15)") {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, "{:?}", error),
        };
    });

    // Wait for `SELECT sleep(5)` to become running
    std::thread::sleep(Duration::from_secs(5));

    session.abort(false)?;
    match session.wait_terminal(Some(Duration::from_secs(5))).await {
        Ok(_) => assert!(false, "wait_terminal must be return timeout."),
        Err(error) => {
            assert_eq!(error.code(), 40);
            assert_eq!(error.message(), "Session did not close in 5s");
        }
    };

    assert!(instant.elapsed().gt(&Duration::from_secs(10)));
    assert!(instant.elapsed().lt(&Duration::from_secs(15)));
    query_join_handler.await.unwrap();
    assert!(instant.elapsed().gt(&Duration::from_secs(15)));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_progress_wait_terminal_before_force_abort() -> Result<()> {
    let instant = Instant::now();
    let (mut conn, session) = prepare_session_and_connect().await?;

    let query_join_handler = tokio::spawn(async move {
        conn.query::<Vec<u8>, &str>("SET max_threads = 1").unwrap();
        conn.query::<Vec<u8>, &str>("SET max_block_size = 1")
            .unwrap();

        match conn.query::<Vec<u8>, &str>("SELECT sleep(1) FROM numbers(15)") {
            Ok(_) => assert!(false, "SELECT sleep(1) FROM numbers(15) must be timeout."),
            Err(error) => {
                assert_eq!(error.to_string(), "MySqlError { ERROR 1152 (08S01): Code: 43, displayText = Aborted query, because the server is shutting down or the query was killed. }");
            }
        };
    });

    // Wait for `SELECT sleep(5)` to become running
    std::thread::sleep(Duration::from_secs(5));

    let wait_terminal_session = session.clone();
    let wait_terminal_session_join_handle = tokio::spawn(async move {
        match wait_terminal_session.wait_terminal(None).await {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, "wait_terminal must be return Ok. {}", error),
        };
    });

    session.abort(true)?;
    match wait_terminal_session_join_handle.await {
        Ok(_) => assert!(true),
        Err(err) => assert!(false, "wait_terminal error {}", err),
    }

    query_join_handler.await.unwrap();
    assert!(instant.elapsed().le(&Duration::from_secs(15)));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_progress_wait_terminal_after_force_abort() -> Result<()> {
    let instant = Instant::now();
    let (mut conn, session) = prepare_session_and_connect().await?;

    let query_join_handler = tokio::spawn(async move {
        conn.query::<Vec<u8>, &str>("SET max_threads = 1").unwrap();
        conn.query::<Vec<u8>, &str>("SET max_block_size = 1")
            .unwrap();

        match conn.query::<Vec<u8>, &str>("SELECT sleep(1) FROM numbers(15)") {
            Ok(_) => assert!(false, "SELECT sleep(1) FROM numbers(15) must be timeout."),
            Err(error) => {
                assert_eq!(error.to_string(), "MySqlError { ERROR 1152 (08S01): Code: 43, displayText = Aborted query, because the server is shutting down or the query was killed. }");
            }
        };
    });

    // Wait for `SELECT sleep(5)` to become running
    std::thread::sleep(Duration::from_secs(5));

    session.abort(true)?;
    match session.wait_terminal(None).await {
        Ok(_) => assert!(true),
        Err(error) => assert!(false, "wait_terminal must be return Ok. {}", error),
    };

    // test wait_terminal again.
    match session.wait_terminal(None).await {
        Ok(_) => assert!(true),
        Err(error) => assert!(false, "wait_terminal must be return Ok. {}", error),
    };

    query_join_handler.await.unwrap();
    assert!(instant.elapsed().le(&Duration::from_secs(15)));

    Ok(())
}

async fn prepare_session_and_connect() -> Result<(mysql::Conn, Arc<Box<dyn ISession>>)> {
    let session_manager = SessionMgr::from_conf(Config::default())?;
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let local_addr = listener
        .local_addr()
        .map_err_to_code(ErrorCode::TokioError, || "Tokio Error");

    let session = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let session = session_manager.create_session::<Session>()?;
        session.start(stream).await?;
        Result::Ok(session)
    });

    let conn = tokio::spawn(async move { create_connection(local_addr?.port()) });

    // connect success
    let conn = conn
        .await
        .map_err_to_code(ErrorCode::TokioError, || "Tokio Error")??;

    let session = session
        .await
        .map_err_to_code(ErrorCode::TokioError, || "Tokio Error")??;
    Ok((conn, session))
}

fn create_connection(port: u16) -> Result<mysql::Conn> {
    let uri = &format!("mysql://127.0.0.1:{}", port);
    mysql::Conn::new(uri).map_err_to_code(ErrorCode::UnknownException, || "Reject connection")
}
