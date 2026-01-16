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

use std::io;
use std::net::Shutdown;
use std::sync::Arc;

use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::Thread;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_users::UserApiProvider;
use databend_storages_common_session::drop_all_temp_tables;
use log::error;
use log::info;
use log::warn;
use opensrv_mysql::AsyncMysqlIntermediary;
use opensrv_mysql::ErrorKind;
use opensrv_mysql::IntermediaryOptions;
use opensrv_mysql::ServerHandshakeConfig;
use opensrv_mysql::plain_run_with_options;
use opensrv_mysql::secure_run_with_options;
use rand::RngCore;
use rustls::ServerConfig;
use socket2::SockRef;
use socket2::TcpKeepalive;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;

use crate::servers::mysql::MYSQL_VERSION;
use crate::servers::mysql::mysql_interactive_worker::InteractiveWorker;
use crate::sessions::Session;
use crate::sessions::SessionManager;

// default size of resultset write buffer: 100KB
const DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE: usize = 100 * 1024;

pub struct MySQLConnection;

impl MySQLConnection {
    pub async fn run_on_stream(
        session_manager: Arc<SessionManager>,
        stream: TcpStream,
        keepalive: TcpKeepalive,
        tls: Option<Arc<ServerConfig>>,
    ) -> Result<()> {
        let blocking_stream = Self::convert_stream(stream)?;
        let handshake_stream = blocking_stream.try_clone()?;
        let non_blocking_stream = TcpStream::from_std(handshake_stream)?;

        let client_addr = match non_blocking_stream.peer_addr() {
            Ok(addr) => addr.to_string(),
            Err(e) => {
                warn!(
                    "Failed to get mysql conn peer address for {:?}: {}",
                    non_blocking_stream, e
                );
                return Ok(());
            }
        };

        let (reader, writer_half) = non_blocking_stream.into_split();
        let mut writer =
            BufWriter::with_capacity(DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE, writer_half);

        let version = GlobalConfig::version();
        let version_string = format!("{MYSQL_VERSION}-{}", version.commit_detail);
        let mysql_conn_id = session_manager.alloc_mysql_conn_id();
        let salt = Self::generate_salt();

        let handshake_config = ServerHandshakeConfig {
            version: version_string,
            connection_id: mysql_conn_id,
            default_auth_plugin: "mysql_native_password".to_string(),
            scramble: salt,
        };

        let handshake_result = AsyncMysqlIntermediary::<
            InteractiveWorker,
            OwnedReadHalf,
            BufWriter<OwnedWriteHalf>,
        >::init_before_ssl_with_config(
            &handshake_config, reader, &mut writer, &tls
        )
        .await;

        let (use_ssl, init_params) = match handshake_result {
            Ok(res) => res,
            Err(error) if Self::connection_terminated(&error) => return Ok(()),
            Err(error) => {
                return Err(ErrorCode::TokioError(format!(
                    "Handshaking mysql connection failed: {error}"
                )));
            }
        };

        let response_seq = init_params.1;

        let session = match session_manager
            .create_mysql_session_with_conn_id(mysql_conn_id)
            .await
        {
            Ok(session) => session,
            Err(error) => {
                warn!("create session failed, {:?}", error);
                Self::send_session_error(&mut writer, response_seq, &error).await;
                return Ok(());
            }
        };

        let session = match session_manager.register_session(session) {
            Ok(session) => session,
            Err(error) => {
                warn!("fail to register session, {:?}", error);
                Self::send_session_error(&mut writer, response_seq, &error).await;
                return Ok(());
            }
        };

        if let Err(e) = SockRef::from(&blocking_stream).set_tcp_keepalive(&keepalive) {
            warn!("failed to set socket option keepalive {}", e);
        }

        MySQLConnection::attach_session(&session, &blocking_stream)?;
        drop(blocking_stream);

        info!("MySQL connection coming: {}", client_addr);

        let query_executor =
            Runtime::with_worker_threads(1, Some("mysql-query-executor".to_string()))?;

        Thread::spawn(move || {
            let tls_clone = tls.clone();
            let interactive_worker =
                InteractiveWorker::create(session.clone(), version, client_addr, salt);
            let opts = IntermediaryOptions {
                process_use_statement_on_query: true,
                reject_connection_on_dbname_absence: false,
            };

            let join_handle = query_executor.spawn(
                async move {
                    let run_result = match (tls_clone, use_ssl) {
                        (Some(config), true) => {
                            secure_run_with_options(
                                interactive_worker,
                                writer,
                                opts,
                                config,
                                init_params,
                            )
                            .await
                        }
                        _ => {
                            plain_run_with_options(interactive_worker, writer, opts, init_params)
                                .await
                        }
                    };

                    run_result.ok();

                    let tenant = session.get_current_tenant();
                    let session_id = session.get_id();
                    let user = session.get_current_user()?.name;
                    UserApiProvider::instance()
                        .client_session_api(&tenant)
                        .drop_client_session_id(&session_id, &user)
                        .await
                        .ok();
                    drop_all_temp_tables(
                        &format!("{user}/{session_id}"),
                        session.temp_tbl_mgr(),
                        "mysql",
                    )
                    .await
                },
                None,
            );

            let _ = futures::executor::block_on(join_handle);
        });
        Ok(())
    }

    fn connection_terminated(error: &io::Error) -> bool {
        matches!(
            error.kind(),
            io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::BrokenPipe
        )
    }

    async fn send_session_error(
        writer: &mut BufWriter<OwnedWriteHalf>,
        seq: u8,
        error: &ErrorCode,
    ) {
        if let Err(e) = Self::write_error_packet(writer, seq, error).await {
            warn!("failed to send mysql error packet: {e}");
        }
    }

    async fn write_error_packet(
        writer: &mut BufWriter<OwnedWriteHalf>,
        seq: u8,
        error: &ErrorCode,
    ) -> io::Result<()> {
        let (kind, message) = Self::map_error_kind(error);
        let mut payload = Vec::with_capacity(1 + 2 + 1 + 5 + message.len());
        payload.push(0xFF);
        payload.extend_from_slice(&(kind as u16).to_le_bytes());
        payload.push(b'#');
        payload.extend_from_slice(kind.sqlstate());
        payload.extend_from_slice(message.as_bytes());

        let payload_len = payload.len() as u32;
        let mut header = [0u8; 4];
        header[0] = (payload_len & 0xFF) as u8;
        header[1] = ((payload_len >> 8) & 0xFF) as u8;
        header[2] = ((payload_len >> 16) & 0xFF) as u8;
        header[3] = seq.wrapping_add(1);

        writer.write_all(&header).await?;
        writer.write_all(&payload).await?;
        writer.flush().await
    }

    fn map_error_kind(error: &ErrorCode) -> (ErrorKind, String) {
        match error.code() {
            41 => (ErrorKind::ER_TOO_MANY_USER_CONNECTIONS, error.message()),
            _ => (ErrorKind::ER_INTERNAL_ERROR, error.message()),
        }
    }

    fn attach_session(session: &Arc<Session>, blocking_stream: &std::net::TcpStream) -> Result<()> {
        let host = blocking_stream.peer_addr().ok();
        let blocking_stream_ref = blocking_stream.try_clone()?;
        session.attach(host, move || {
            if let Err(error) = blocking_stream_ref.shutdown(Shutdown::Both) {
                error!("Cannot shutdown MySQL session io {}", error);
            }
        });

        Ok(())
    }

    fn generate_salt() -> [u8; 20] {
        let mut bs = vec![0u8; 20];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(bs.as_mut());

        let mut scramble: [u8; 20] = [0; 20];
        for i in 0..20 {
            scramble[i] = bs[i] & 0x7fu8;
            if scramble[i] == b'\0' || scramble[i] == b'$' {
                scramble[i] += 1;
            }
        }

        scramble
    }

    // TODO: move to ToBlockingStream trait
    fn convert_stream(stream: TcpStream) -> Result<std::net::TcpStream> {
        let stream = stream.into_std().map_err_to_code(
            ErrorCode::TokioError,
            || "Cannot to convert Tokio TcpStream to Std TcpStream",
        )?;

        Ok(stream)
    }
}
