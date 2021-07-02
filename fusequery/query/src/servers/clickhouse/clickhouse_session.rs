use std::net::Shutdown;
use std::sync::Arc;

use clickhouse_srv::ClickHouseServer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_runtime::tokio;
use common_runtime::tokio::net::TcpStream;
use common_runtime::Runtime;

use crate::servers::clickhouse::interactive_worker::InteractiveWorker;
use crate::sessions::SessionRef;

pub struct ClickHouseConnection;

impl ClickHouseConnection {
    pub fn run_on_stream(session: SessionRef, stream: TcpStream) -> Result<()> {
        let blocking_stream = Self::convert_stream(stream)?;
        ClickHouseConnection::attach_session(&session, &blocking_stream)?;
        let non_blocking_stream = TcpStream::from_std(blocking_stream)?;

        tokio::spawn(async move {
            let interactive_worker = InteractiveWorker::create(session);
            ClickHouseServer::run_on_stream(interactive_worker, non_blocking_stream)
        });

        Ok(())
    }

    fn attach_session(session: &SessionRef, blocking_stream: &std::net::TcpStream) -> Result<()> {
        let host = blocking_stream.peer_addr().ok();
        let blocking_stream_ref = blocking_stream.try_clone()?;
        session.attach(host, move || {
            if let Err(error) = blocking_stream_ref.shutdown(Shutdown::Both) {
                log::error!("Cannot shutdown ClickHouse session io {}", error);
            }
        });

        Ok(())
    }

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
