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

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::BuildInfoRef;
use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SendableDataBlockStream;
use databend_common_io::prelude::FormatSettings;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_metrics::mysql::*;
use databend_common_users::CertifiedInfo;
use databend_common_users::UserApiProvider;
use fastrace::func_path;
use fastrace::prelude::*;
use futures_util::StreamExt;
use log::error;
use log::info;
use opensrv_mysql::AsyncMysqlShim;
use opensrv_mysql::ErrorKind;
use opensrv_mysql::InitWriter;
use opensrv_mysql::ParamParser;
use opensrv_mysql::QueryResultWriter;
use opensrv_mysql::StatementMetaWriter;
use rand::Rng as _;
use rand::thread_rng;
use tokio::io::AsyncWrite;
use uuid::Uuid;

use crate::auth::CredentialType;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::interpreter_plan_sql;
use crate::servers::login_history::LoginEventType;
use crate::servers::login_history::LoginHandler;
use crate::servers::login_history::LoginHistory;
use crate::servers::mysql::MYSQL_VERSION;
use crate::servers::mysql::MySQLFederated;
use crate::servers::mysql::writers::DFInitResultWriter;
use crate::servers::mysql::writers::DFQueryResultWriter;
use crate::servers::mysql::writers::ProgressReporter;
use crate::servers::mysql::writers::QueryResult;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContext;
use crate::stream::DataBlockStream;

struct InteractiveWorkerBase {
    session: Arc<Session>,
    version: BuildInfoRef,
}

pub struct InteractiveWorker {
    base: InteractiveWorkerBase,
    version: String,
    salt: [u8; 20],
    client_addr: String,
    keep_alive_task_started: bool,
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Sync + Unpin> AsyncMysqlShim<W> for InteractiveWorker {
    type Error = ErrorCode;

    fn version(&self) -> String {
        self.version.clone()
    }

    fn connect_id(&self) -> u32 {
        match self.base.session.get_mysql_conn_id() {
            Some(conn_id) => conn_id,
            None => {
                // default conn id
                u32::from_le_bytes([0x08, 0x00, 0x00, 0x00])
            }
        }
    }

    fn default_auth_plugin(&self) -> &str {
        "mysql_native_password"
    }

    #[async_backtrace::framed]
    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &'static str {
        "mysql_native_password"
    }

    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    #[async_backtrace::framed]
    async fn authenticate(
        &self,
        _auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        let mut login_history = LoginHistory::new();
        login_history.handler = LoginHandler::MySQL;

        let username = String::from_utf8_lossy(username);
        let client_addr = self.client_addr.clone();
        login_history.user_name = username.to_string();
        login_history.auth_type = CredentialType::Password;
        login_history.client_ip = client_addr.clone();
        login_history.node_id = GlobalConfig::instance().query.node_id.clone();
        login_history.session_id = self.base.session.get_id().to_string();

        let info = CertifiedInfo::create(&username, auth_data, &client_addr);

        let authenticate = self.base.authenticate(salt, info);
        match authenticate.await {
            Ok(res) => {
                login_history.event_type = LoginEventType::LoginSuccess;
                login_history.write_to_log();
                res
            }
            Err(failure) => {
                login_history.event_type = LoginEventType::LoginFailed;
                login_history.error_message = failure.to_string();
                login_history.write_to_log();
                error!(
                    "MySQL handler authenticate failed, \
                        user_name: {}, \
                        client_address: {}, \
                        failure_cause: {}",
                    username, client_addr, failure
                );
                false
            }
        }
    }

    #[async_backtrace::framed]
    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        writer: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        if self.base.session.is_aborting() {
            writer
                .error(
                    ErrorKind::ER_ABORTING_CONNECTION,
                    "Aborting this connection. because we are try aborting server.".as_bytes(),
                )
                .await?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        self.base.do_prepare(query, writer).await
    }

    #[async_backtrace::framed]
    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        param: ParamParser<'a>,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        if self.base.session.is_aborting() {
            writer
                .error(
                    ErrorKind::ER_ABORTING_CONNECTION,
                    "Aborting this connection. because we are try aborting server.".as_bytes(),
                )
                .await?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        self.base.do_execute(id, param, writer).await
    }

    /// https://dev.mysql.com/doc/internals/en/com-stmt-close.html
    #[async_backtrace::framed]
    async fn on_close<'a>(&'a mut self, stmt_id: u32)
    where W: 'async_trait {
        self.base.do_close(stmt_id).await;
    }

    #[async_backtrace::framed]
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        let query_id = Uuid::now_v7();
        // Ensure the query id shares the same representation as trace_id.
        let query_id_str = query_id.simple().to_string();
        let sampled =
            thread_rng().gen_range(0..100) <= self.base.session.get_trace_sample_rate()?;
        let span_context =
            SpanContext::new(TraceId(query_id.as_u128()), SpanId::default()).sampled(sampled);
        let root = Span::root(func_path!(), span_context)
            .with_properties(|| self.base.session.to_fastrace_properties());

        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.query_id = Some(query_id_str.clone());
        tracking_payload.mem_stat = Some(MemStat::create(query_id_str.to_string()));
        let _guard = ThreadTracker::tracking(tracking_payload);

        ThreadTracker::tracking_future(async {
            if self.base.session.is_aborting() {
                writer
                    .error(
                        ErrorKind::ER_ABORTING_CONNECTION,
                        "Aborting this connection. because we are try aborting server.".as_bytes(),
                    )
                    .await?;

                return Err(ErrorCode::AbortedSession(
                    "Aborting this connection. because we are try aborting server.",
                ));
            }

            let mut writer = DFQueryResultWriter::create(writer, self.base.session.clone());
            if !self.keep_alive_task_started {
                self.start_keep_alive().await
            }

            let instant = Instant::now();
            let query_result = self
                .base
                .do_query(query_id_str, query)
                .await
                .map_err(|err| err.display_with_sql(query));

            let format = self.base.session.get_format_settings();

            let mut write_result = writer.write(query_result, &format).await;

            if let Err(cause) = write_result {
                self.base.session.txn_mgr().lock().set_fail();
                let suffix = format!("(while in query {})", query);
                write_result = Err(cause.add_message_back(suffix));
            }
            observe_mysql_process_request_duration(instant.elapsed());

            write_result
        })
        .in_span(root)
        .await
    }

    #[async_backtrace::framed]
    async fn on_init<'a>(
        &'a mut self,
        database_name: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<()> {
        if self.base.session.is_aborting() {
            writer
                .error(
                    ErrorKind::ER_ABORTING_CONNECTION,
                    "Aborting this connection. because we are try aborting server.".as_bytes(),
                )
                .await?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        DFInitResultWriter::create(writer)
            .write(self.base.do_init(database_name).await)
            .await
    }
}

impl InteractiveWorkerBase {
    #[async_backtrace::framed]
    async fn authenticate(&self, salt: &[u8], info: CertifiedInfo) -> Result<bool> {
        let user_api = UserApiProvider::instance();
        let ctx = self.session.create_query_context(self.version).await?;
        let tenant = ctx.get_tenant();
        let identity = UserIdentity::new(&info.user_name, "%");
        let client_ip = info.user_client_address.split(':').collect::<Vec<_>>()[0];
        let mut user = user_api
            .get_user_with_client_ip(&tenant, identity.clone(), Some(client_ip))
            .await?;

        // check global network policy if user is not account admin
        if !user.is_account_admin() {
            let global_network_policy = ctx.get_settings().get_network_policy().unwrap_or_default();
            if !global_network_policy.is_empty() {
                user_api
                    .enforce_network_policy(&tenant, &global_network_policy, Some(client_ip))
                    .await?;
            }
        }

        // Check password policy for login
        let need_change = user_api
            .check_login_password(&tenant, identity.clone(), &user)
            .await?;
        if need_change {
            user.update_auth_need_change_password();
        }

        let authed = user.auth_info.auth_mysql(&info.user_password, salt)?;
        user_api
            .update_user_login_result(tenant, identity, authed, &user)
            .await?;
        if authed {
            self.session.set_authed_user(user, None).await?;
        }
        Ok(authed)
    }

    #[async_backtrace::framed]
    async fn do_prepare<W: AsyncWrite + Unpin>(
        &mut self,
        _: &str,
        writer: StatementMetaWriter<'_, W>,
    ) -> Result<()> {
        writer
            .error(
                ErrorKind::ER_UNKNOWN_ERROR,
                "Prepare is not support in Databend.".as_bytes(),
            )
            .await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn do_execute<W: AsyncWrite + Unpin>(
        &mut self,
        _: u32,
        _: ParamParser<'_>,
        writer: QueryResultWriter<'_, W>,
    ) -> Result<()> {
        writer
            .error(
                ErrorKind::ER_UNKNOWN_ERROR,
                "Execute is not support in Databend.".as_bytes(),
            )
            .await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn do_close(&mut self, _: u32) {}

    // Check the query is a federated or driver setup command.
    // Here we fake some values for the command which Databend not supported.
    fn federated_server_command_check(&self, query: &str) -> Option<(DataSchemaRef, DataBlock)> {
        // INSERT don't need MySQL federated check
        // Ensure the query is start with ASCII chars so we won't
        // panic when we slice the query string.
        if query.len() > 6
            && query.char_indices().take(6).all(|(_, c)| c.is_ascii())
            && query[..6].eq_ignore_ascii_case("INSERT")
        {
            return None;
        }
        let federated = MySQLFederated::create();
        federated.check(query)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn do_query(
        &mut self,
        query_id: String,
        query: &str,
    ) -> Result<(QueryResult, Option<FormatSettings>)> {
        match self.federated_server_command_check(query) {
            Some((schema, data_block)) => {
                info!("Federated query: {}", query);
                if data_block.num_rows() > 0 {
                    info!("Federated response: {:?}", data_block);
                }
                // Fix for MySQL ODBC 9.5 + PowerBI compatibility:
                // Empty result sets (0 rows but with schema) must set has_result=true,
                // otherwise ODBC throws "invalid cursor state" error on commands like "SHOW KEYS FROM table".
                // Check for column definitions instead of row count.
                let has_result = !schema.fields().is_empty() || data_block.num_columns() > 0;
                Ok((
                    QueryResult::create(
                        DataBlockStream::create(None, vec![data_block]).boxed(),
                        None,
                        has_result,
                        schema,
                        query.to_string(),
                    ),
                    None,
                ))
            }
            None => {
                info!("Normal query: {}", query);
                let context = self.session.create_query_context(self.version).await?;
                context.update_init_query_id(query_id);

                // Use interpreter_plan_sql, we can write the query log if an error occurs.
                let (plan, _, _guard) = interpreter_plan_sql(context.clone(), query, true).await?;

                let interpreter = InterpreterFactory::get(context.clone(), &plan).await?;
                let has_result_set = plan.has_result_set();

                let (blocks, extra_info) = Self::exec_query(interpreter.clone(), &context).await?;
                let mut schema = plan.schema();
                if let Some(real_schema) = interpreter.get_dynamic_schema().await {
                    schema = real_schema;
                }

                let format = context.get_format_settings()?;
                Ok((
                    QueryResult::create(
                        blocks,
                        extra_info,
                        has_result_set,
                        schema,
                        query.to_string(),
                    ),
                    Some(format),
                ))
            }
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn exec_query(
        interpreter: Arc<dyn Interpreter>,
        context: &Arc<QueryContext>,
    ) -> Result<(
        SendableDataBlockStream,
        Option<Box<dyn ProgressReporter + Send>>,
    )> {
        let instant = Instant::now();

        let query_result = context.try_spawn(
            {
                let ctx = context.clone();
                async move {
                    let mut data_stream = interpreter.execute(ctx.clone()).await?;
                    observe_mysql_interpreter_used_time(instant.elapsed());

                    // Wrap the data stream, log finish event at the end of stream
                    let intercepted_stream = async_stream::stream! {

                        while let Some(item) = data_stream.next().await {
                            yield item
                        };
                    };

                    Ok::<_, ErrorCode>(intercepted_stream.boxed())
                }
                .in_span(Span::enter_with_local_parent(func_path!()))
            },
            None,
        )?;

        let query_result = query_result.await.map_err_to_code(
            ErrorCode::TokioError,
            || "Cannot join handle from context's runtime",
        )?;
        let reporter = Box::new(ContextProgressReporter::new(context.clone(), instant))
            as Box<dyn ProgressReporter + Send>;
        query_result.map(|data| (data, Some(reporter)))
    }

    #[async_backtrace::framed]
    async fn do_init(&mut self, database_name: &str) -> Result<()> {
        if database_name.is_empty() {
            return Ok(());
        }

        let query_id = Uuid::new_v4().to_string();
        let init_query = format!("USE `{}`;", database_name);

        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.query_id = Some(query_id.clone());
        tracking_payload.mem_stat = Some(MemStat::create(query_id.clone()));
        let _guard = ThreadTracker::tracking(tracking_payload);

        let do_query = ThreadTracker::tracking_future(self.do_query(query_id, &init_query)).await;
        match do_query {
            Ok((_, _)) => Ok(()),
            Err(error_code) => Err(error_code),
        }
    }
}

impl InteractiveWorker {
    pub fn create(
        session: Arc<Session>,
        version: BuildInfoRef,
        client_addr: String,
        salt: [u8; 20],
    ) -> InteractiveWorker {
        InteractiveWorker {
            version: format!("{MYSQL_VERSION}-{}", version.commit_detail),
            base: InteractiveWorkerBase { session, version },
            salt,
            client_addr,
            keep_alive_task_started: false,
        }
    }

    async fn start_keep_alive(&mut self) {
        let session = &self.base.session;
        let tenant = session.get_current_tenant();
        let session_id = session.get_id();
        let user_name = session
            .get_current_user()
            .expect("mysql handler should be authed when call")
            .name;
        self.keep_alive_task_started = true;

        databend_common_base::runtime::spawn(async move {
            loop {
                UserApiProvider::instance()
                    .client_session_api(&tenant)
                    .upsert_client_session_id(
                        &session_id,
                        &user_name,
                        Duration::from_secs(3600 + 600),
                    )
                    .await
                    .ok();
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
    }
}

struct ContextProgressReporter {
    context: Arc<QueryContext>,
    instant: Instant,
}

impl ContextProgressReporter {
    fn new(context: Arc<QueryContext>, instant: Instant) -> Self {
        Self { context, instant }
    }
}

impl ProgressReporter for ContextProgressReporter {
    fn progress_info(&self) -> String {
        let progress = self.context.get_scan_progress_value();
        let seconds = self.instant.elapsed().as_nanos() as f64 / 1e9f64;
        format!(
            "Read {} rows, {} in {:.3} sec., {} rows/sec., {}/sec.",
            progress.rows,
            convert_byte_size(progress.bytes as f64),
            seconds,
            convert_number_size((progress.rows as f64) / (seconds)),
            convert_byte_size((progress.bytes as f64) / (seconds)),
        )
    }

    fn affected_rows(&self) -> u64 {
        let progress = self.context.get_write_progress_value();
        progress.rows as u64
    }
}
