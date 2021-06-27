// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use msql_srv::MysqlIntermediary;

use common_exception::ErrorCode;
use common_exception::exception::ABORT_SESSION;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_infallible::exit_scope;
use common_infallible::Mutex;
use common_runtime::tokio;
use common_runtime::tokio::net::TcpStream;

use crate::configs::Config;
use crate::servers::AbortableService;
use crate::servers::Elapsed;
use crate::servers::mysql::mysql_interactive_worker::InteractiveWorker;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionRef;
use crate::sessions::SessionManagerRef;
use crate::sessions::Session;
use std::net::Shutdown;
use futures::{TryFutureExt, FutureExt};

pub struct MySQLConnection;

impl MySQLConnection {
    pub fn run_on_stream(session: SessionRef, stream: TcpStream) -> Result<()> {
        let blocking_stream = Self::convert_stream(stream)?;

        let host = blocking_stream.peer_addr().ok();
        let blocking_stream_ref = blocking_stream.try_clone()?;
        session.attach(host, move || { blocking_stream_ref.shutdown(Shutdown::Both); });

        std::thread::spawn(move || {
            if let Err(error) =
            MysqlIntermediary::run_on_tcp(InteractiveWorker::create(session), blocking_stream)
            {
                if error.code() != ABORT_SESSION {
                    log::error!("Unexpected error occurred during query execution: {:?}", error);
                }
            };
        });

        Ok(())
    }

    // TODO: move to ToBlockingStream trait
    fn convert_stream(stream: TcpStream) -> Result<std::net::TcpStream> {
        let stream = stream
            .into_std()
            .map_err_to_code(ErrorCode::TokioError, || {
                "Cannot to convert Tokio TcpStream to Std TcpStream"
            })?;
        stream
            .set_nonblocking(false)
            .map_err_to_code(ErrorCode::TokioError, || {
                "Cannot to convert Tokio TcpStream to Std TcpStream"
            })?;

        Ok(stream)
    }
}
