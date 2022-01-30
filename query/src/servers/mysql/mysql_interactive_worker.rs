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

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use common_planners::PlanNode;
use common_tracing::tracing;
use metrics::histogram;
use msql_srv::AsyncMysqlShim;
use msql_srv::ErrorKind;
use msql_srv::InitWriter;
use msql_srv::ParamParser;
use msql_srv::QueryResultWriter;
use msql_srv::StatementMetaWriter;
use rand::RngCore;
use regex::RegexSet;
use tokio_stream::StreamExt;

use crate::interpreters::InterpreterFactory;
use crate::servers::mysql::writers::DFInitResultWriter;
use crate::servers::mysql::writers::DFQueryResultWriter;
use crate::sessions::QueryContext;
use crate::sessions::SessionRef;
use crate::sql::PlanParser;
use crate::users::CertifiedInfo;

struct InteractiveWorkerBase<W: std::io::Write> {
    session: SessionRef,
    generic_hold: PhantomData<W>,
}

pub struct InteractiveWorker<W: std::io::Write> {
    session: SessionRef,
    base: InteractiveWorkerBase<W>,
    version: String,
    salt: [u8; 20],
    client_addr: String,
}

#[async_trait::async_trait]
impl<W: std::io::Write + Send> AsyncMysqlShim<W> for InteractiveWorker<W> {
    type Error = ErrorCode;

    fn version(&self) -> &str {
        self.version.as_str()
    }

    fn connect_id(&self) -> u32 {
        u32::from_le_bytes([0x08, 0x00, 0x00, 0x00])
    }

    fn default_auth_plugin(&self) -> &str {
        "mysql_native_password"
    }

    fn auth_plugin_for_username(&self, _user: &[u8]) -> &str {
        "mysql_native_password"
    }

    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    fn authenticate(
        &self,
        _auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        let username = String::from_utf8_lossy(username);
        let info = CertifiedInfo::create(&username, auth_data, &self.client_addr);

        let authenticate = self.base.authenticate(salt, info);
        futures::executor::block_on(async move {
            match authenticate.await {
                Ok(res) => res,
                Err(failure) => {
                    tracing::error!(
                        "MySQL handler authenticate failed, \
                        user_name: {}, \
                        client_address: {}, \
                        failure_cause: {}",
                        username,
                        self.client_addr,
                        failure
                    );
                    false
                }
            }
        })
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        writer: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        if self.session.is_aborting() {
            writer.error(
                ErrorKind::ER_ABORTING_CONNECTION,
                "Aborting this connection. because we are try aborting server.".as_bytes(),
            )?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        self.base.do_prepare(query, writer).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        param: ParamParser<'a>,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        if self.session.is_aborting() {
            writer.error(
                ErrorKind::ER_ABORTING_CONNECTION,
                "Aborting this connection. because we are try aborting server.".as_bytes(),
            )?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        self.base.do_execute(id, param, writer).await
    }

    async fn on_close<'a>(&'a mut self, id: u32)
    where W: 'async_trait {
        self.base.do_close(id).await;
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        if self.session.is_aborting() {
            writer.error(
                ErrorKind::ER_ABORTING_CONNECTION,
                "Aborting this connection. because we are try aborting server.".as_bytes(),
            )?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        let mut writer = DFQueryResultWriter::create(writer);

        let instant = Instant::now();
        let blocks = self.base.do_query(query).await;

        let mut write_result = writer.write(blocks);

        if let Err(cause) = write_result {
            let suffix = format!("(while in query {})", query);
            write_result = Err(cause.add_message_back(suffix));
        }

        histogram!(
            super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
            instant.elapsed()
        );

        write_result
    }

    async fn on_init<'a>(
        &'a mut self,
        database_name: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<()> {
        if self.session.is_aborting() {
            writer.error(
                ErrorKind::ER_ABORTING_CONNECTION,
                "Aborting this connection. because we are try aborting server.".as_bytes(),
            )?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        DFInitResultWriter::create(writer).write(self.base.do_init(database_name).await)
    }
}

impl<W: std::io::Write> InteractiveWorkerBase<W> {
    async fn authenticate(&self, salt: &[u8], info: CertifiedInfo) -> Result<bool> {
        let user_name = &info.user_name;
        let user_manager = self.session.get_user_manager();
        let client_ip = info.user_client_address.split(':').collect::<Vec<_>>()[0];

        let ctx = self.session.create_query_context().await?;
        let user_info = user_manager
            .get_user_with_client_ip(&ctx.get_tenant(), user_name, client_ip)
            .await?;

        let authed = user_info.auth_info.auth_mysql(&info.user_password, salt)?;
        if authed {
            self.session.set_current_user(user_info);
        }
        Ok(authed)
    }

    async fn do_prepare(&mut self, _: &str, writer: StatementMetaWriter<'_, W>) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Prepare is not support in Databend.".as_bytes(),
        )?;
        Ok(())
    }

    async fn do_execute(
        &mut self,
        _: u32,
        _: ParamParser<'_>,
        writer: QueryResultWriter<'_, W>,
    ) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Execute is not support in Databend.".as_bytes(),
        )?;
        Ok(())
    }

    async fn do_close(&mut self, _: u32) {}

    fn federated_server_setup_set_or_jdbc_command(&mut self, query: &str) -> bool {
        let expr = RegexSet::new(&[
            "(?i)^(SET NAMES(.*))",
            "(?i)^(SET character_set_results(.*))",
            "(?i)^(SET FOREIGN_KEY_CHECKS(.*))",
            "(?i)^(SET AUTOCOMMIT(.*))",
            "(?i)^(SET sql_mode(.*))",
            "(?i)^(SET @@(.*))",
            "(?i)^(SET SESSION TRANSACTION ISOLATION LEVEL(.*))",
            // Just compatibility for jdbc
            "(?i)^(/\\* mysql-connector-java(.*))",
        ])
        .unwrap();
        expr.is_match(query)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn do_query(&mut self, query: &str) -> Result<(Vec<DataBlock>, String)> {
        tracing::debug!("{}", query);

        if self.federated_server_setup_set_or_jdbc_command(query) {
            Ok((vec![DataBlock::empty()], String::from("")))
        } else {
            let context = self.session.create_query_context().await?;
            context.attach_query_str(query);
            let (plan, hints) = PlanParser::parse_with_hint(query, context.clone()).await;

            match hints
                .iter()
                .find(|v| v.error_code.is_some())
                .and_then(|x| x.error_code)
            {
                None => Self::exec_query(plan, &context).await,
                Some(hint_error_code) => match Self::exec_query(plan, &context).await {
                    Ok(_) => Err(ErrorCode::UnexpectedError(format!(
                        "Expected server error code: {} but got: Ok.",
                        hint_error_code
                    ))),
                    Err(error_code) => {
                        if hint_error_code == error_code.code() {
                            Ok((vec![DataBlock::empty()], String::from("")))
                        } else {
                            let actual_code = error_code.code();
                            Err(error_code.add_message(format!(
                                "Expected server error code: {} but got: {}.",
                                hint_error_code, actual_code
                            )))
                        }
                    }
                },
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(plan, context))]
    async fn exec_query(
        plan: Result<PlanNode>,
        context: &Arc<QueryContext>,
    ) -> Result<(Vec<DataBlock>, String)> {
        let instant = Instant::now();

        let interpreter = InterpreterFactory::get(context.clone(), plan?)?;
        // Write start query log.
        let _ = interpreter
            .start()
            .await
            .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));
        let data_stream = interpreter.execute(None).await?;
        histogram!(
            super::mysql_metrics::METRIC_INTERPRETER_USEDTIME,
            instant.elapsed()
        );

        let collector = data_stream.collect::<Result<Vec<DataBlock>>>();
        let query_result = collector.await;
        // Write finish query log.
        let _ = interpreter
            .finish()
            .await
            .map_err(|e| tracing::error!("interpreter.finish.error: {:?}", e));
        query_result.map(|data| (data, Self::extra_info(context, instant)))
    }

    fn extra_info(context: &Arc<QueryContext>, instant: Instant) -> String {
        let progress = context.get_scan_progress_value();
        let seconds = instant.elapsed().as_nanos() as f64 / 1e9f64;
        format!(
            "Read {} rows, {} in {:.3} sec., {} rows/sec., {}/sec.",
            progress.read_rows,
            convert_byte_size(progress.read_bytes as f64),
            seconds,
            convert_number_size((progress.read_rows as f64) / (seconds as f64)),
            convert_byte_size((progress.read_bytes as f64) / (seconds as f64)),
        )
    }

    async fn do_init(&mut self, database_name: &str) -> Result<()> {
        let init_query = format!("USE `{}`;", database_name);

        let do_query = self.do_query(&init_query).await;
        match do_query {
            Ok(_) => Ok(()),
            Err(error_code) => Err(error_code),
        }
    }
}

impl<W: std::io::Write> InteractiveWorker<W> {
    pub fn create(session: SessionRef, client_addr: String) -> InteractiveWorker<W> {
        let mut bs = vec![0u8; 20];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(bs.as_mut());

        let mut scramble: [u8; 20] = [0; 20];
        for i in 0..20 {
            scramble[i] = bs[i];
            if scramble[i] == b'\0' || scramble[i] == b'$' {
                scramble[i] += 1;
            }
        }

        InteractiveWorker::<W> {
            session: session.clone(),
            base: InteractiveWorkerBase::<W> {
                session,
                generic_hold: PhantomData::default(),
            },
            salt: scramble,
            // TODO: version
            version: format!("{}-{}", "8.0.26", *crate::configs::DATABEND_COMMIT_VERSION),
            client_addr,
        }
    }
}
