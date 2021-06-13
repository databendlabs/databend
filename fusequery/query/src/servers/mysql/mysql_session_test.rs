use common_exception::{Result, ToErrorCode, ErrorCode};
use crate::servers::mysql::mysql_session::Session;
use crate::sessions::{SessionCreator, SessionManager, ISession};
use crate::configs::Config;
use crate::clusters::Cluster;
use std::time::Duration;
use std::sync::Arc;
use mysql::prelude::Queryable;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idle_state_wait_terminal_with_not_abort() -> Result<()> {
    let (conn, session) = prepare_session_and_connect().await?;

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
    let (conn, session) = prepare_session_and_connect().await?;

    session.abort(false)?;
    match session.wait_terminal(Some(Duration::from_secs(5))).await {
        Ok(_) => assert!(false, "wait_terminal must be return timeout."),
        Err(error) => {
            assert_eq!(error.code(), 40);
            assert_eq!(error.message(), "Session did not close in 5s");
        }
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idle_wait_terminal_before_force_abort() -> Result<()> {
    let (conn, session) = prepare_session_and_connect().await?;

    let wait_terminal_session = session.clone();
    let wait_terminal_session_join_handle = tokio::spawn(async move {
        match wait_terminal_session.wait_terminal(None).await {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, "wait_terminal must be return Ok."),
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
    let (conn, session) = prepare_session_and_connect().await?;

    session.abort(true)?;
    match session.wait_terminal(None).await {
        Ok(_) => assert!(true),
        Err(error) => assert!(false, "wait_terminal must be return Ok."),
    };

    // test wait_terminal again.
    match session.wait_terminal(None).await {
        Ok(_) => assert!(true),
        Err(error) => assert!(false, "wait_terminal must be return Ok."),
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_progress_state_wait_terminal_with_not_abort() -> Result<()> {
    let (mut conn, session) = prepare_session_and_connect().await?;

    tokio::spawn(async move {
        match conn.query::<Vec<u8>, &str>("SELECT sleep(15)") {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, ""),
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

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_progress_wait_terminal_after_not_force_abort() -> Result<()> {
    let (mut conn, session) = prepare_session_and_connect().await?;

    tokio::spawn(async move {
        match conn.query::<Vec<u8>, &str>("SELECT sleep(15)") {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, ""),
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

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_progress_wait_terminal_before_force_abort() -> Result<()> {
    let (mut conn, session) = prepare_session_and_connect().await?;

    tokio::spawn(async move {
        match conn.query::<Vec<u8>, &str>("SELECT sleep(15)") {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, "{:?}", error),
        };
    });

    // Wait for `SELECT sleep(5)` to become running
    std::thread::sleep(Duration::from_secs(5));

    let wait_terminal_session = session.clone();
    let wait_terminal_session_join_handle = tokio::spawn(async move {
        match wait_terminal_session.wait_terminal(None).await {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, "wait_terminal must be return Ok."),
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
async fn test_progress_wait_terminal_after_force_abort() -> Result<()> {
    let (mut conn, session) = prepare_session_and_connect().await?;

    tokio::spawn(async move {
        match conn.query::<Vec<u8>, &str>("SELECT sleep(15)") {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, ""),
        };
    });

    // Wait for `SELECT sleep(5)` to become running
    std::thread::sleep(Duration::from_secs(5));

    session.abort(true)?;
    match session.wait_terminal(None).await {
        Ok(_) => assert!(true),
        Err(error) => assert!(false, "wait_terminal must be return Ok."),
    };

    // test wait_terminal again.
    match session.wait_terminal(None).await {
        Ok(_) => assert!(true),
        Err(error) => assert!(false, "wait_terminal must be return Ok."),
    };

    Ok(())
}


async fn prepare_session_and_connect() -> Result<(mysql::Conn, Arc<Box<dyn ISession>>)> {
    let session_manager = SessionManager::from_conf(Config::default(), Cluster::empty())?;
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let local_addr = listener.local_addr().map_err_to_code(ErrorCode::TokioError, || "");

    let session = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let session = session_manager.create_session::<Session>()?;
        session.start(stream).await?;
        Result::Ok(session)
    });

    let conn = tokio::spawn(async move { create_connection(local_addr?.port()) });

    // connect success
    let conn = conn.await.map_err_to_code(ErrorCode::TokioError, || "")??;

    let session = session.await.map_err_to_code(ErrorCode::TokioError, || "")??;
    Ok((conn, session))
}

fn create_connection(port: u16) -> Result<mysql::Conn> {
    let uri = &format!("mysql://127.0.0.1:{}", port);
    mysql::Conn::new(uri)
        .map_err_to_code(ErrorCode::UnknownException, || "Reject connection")
}