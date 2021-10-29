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

use std::marker::PhantomData;
use std::time::Instant;

use common_base::tokio;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use common_planners::PlanNode;
use metrics::histogram;
use msql_srv::ErrorKind;
use msql_srv::InitWriter;
use msql_srv::MysqlShim;
use msql_srv::ParamParser;
use msql_srv::QueryResultWriter;
use msql_srv::StatementMetaWriter;
use rand::RngCore;
use tokio_stream::StreamExt;

use crate::interpreters::InterpreterFactory;
use crate::servers::mysql::writers::DFInitResultWriter;
use crate::servers::mysql::writers::DFQueryResultWriter;
use crate::sessions::DatabendQueryContextRef;
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

impl<W: std::io::Write> MysqlShim<W> for InteractiveWorker<W> {
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
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        let username = String::from_utf8_lossy(username);
        let info = CertifiedInfo::create(&username, auth_data, &self.client_addr);

        let authenticate = self.base.authenticate(auth_plugin, salt, info);
        futures::executor::block_on(async move {
            match authenticate.await {
                Ok(res) => res,
                Err(failure) => {
                    log::error!(
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

    fn on_prepare(&mut self, query: &str, writer: StatementMetaWriter<W>) -> Result<()> {
        if self.session.is_aborting() {
            writer.error(
                ErrorKind::ER_ABORTING_CONNECTION,
                "Aborting this connection. because we are try aborting server.".as_bytes(),
            )?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        self.base.do_prepare(query, writer)
    }

    fn on_execute(
        &mut self,
        id: u32,
        param: ParamParser,
        writer: QueryResultWriter<W>,
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

        self.base.do_execute(id, param, writer)
    }

    fn on_close(&mut self, id: u32) {
        self.base.do_close(id);
    }

    fn on_query(&mut self, query: &str, writer: QueryResultWriter<W>) -> Result<()> {
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

        match InteractiveWorkerBase::<W>::build_runtime() {
            Ok(runtime) => {
                let instant = Instant::now();
                let blocks = runtime.block_on(self.base.do_query(query));

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
            Err(error) => writer.write(Err(error)),
        }
    }

    fn on_init(&mut self, database_name: &str, writer: InitWriter<W>) -> Result<()> {
        if self.session.is_aborting() {
            writer.error(
                ErrorKind::ER_ABORTING_CONNECTION,
                "Aborting this connection. because we are try aborting server.".as_bytes(),
            )?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        DFInitResultWriter::create(writer).write(self.base.do_init(database_name))
    }
}

impl<W: std::io::Write> InteractiveWorkerBase<W> {
    async fn authenticate(
        &self,
        auth_plugin: &str,
        salt: &[u8],
        info: CertifiedInfo,
    ) -> Result<bool> {
        let user_name = &info.user_name;
        let address = &info.user_client_address;

        let user_manager = self.session.get_user_manager();
        let user_info = user_manager.get_user(user_name).await?;

        let input = &info.user_password;
        let saved = &user_info.password;
        let encode_password = Self::encoding_password(auth_plugin, salt, input, saved)?;

        user_manager
            .auth_user(CertifiedInfo::create(user_name, encode_password, address))
            .await
    }

    fn encoding_password(
        auth_plugin: &str,
        salt: &[u8],
        input: &[u8],
        user_password: &[u8],
    ) -> Result<Vec<u8>> {
        match auth_plugin {
            "mysql_native_password" if input.is_empty() => Ok(vec![]),
            "mysql_native_password" => {
                // SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
                let mut m = sha1::Sha1::new();
                m.update(salt);
                m.update(user_password);

                let result = m.digest().bytes();
                if input.len() != result.len() {
                    return Err(ErrorCode::SHA1CheckFailed("SHA1 check failed"));
                }
                let mut s = Vec::with_capacity(result.len());
                for i in 0..result.len() {
                    s.push(input[i] ^ result[i]);
                }
                Ok(s)
            }
            _ => Ok(input.to_vec()),
        }
    }

    fn do_prepare(&mut self, _: &str, writer: StatementMetaWriter<'_, W>) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Prepare is not support in Databend.".as_bytes(),
        )?;
        Ok(())
    }

    fn do_execute(
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

    fn do_close(&mut self, _: u32) {}

    async fn do_query(&mut self, query: &str) -> Result<(Vec<DataBlock>, String)> {
        log::debug!("{}", query);

        let context = self.session.create_context().await?;
        context.attach_query_str(query);

        let query_parser = PlanParser::create(context.clone());
        let (plan, hints) = query_parser.build_with_hint_from_sql(query);

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

    async fn exec_query(
        plan: Result<PlanNode>,
        context: &DatabendQueryContextRef,
    ) -> Result<(Vec<DataBlock>, String)> {
        let instant = Instant::now();

        let interpreter = InterpreterFactory::get(context.clone(), plan?)?;
        let data_stream = interpreter.execute().await?;
        histogram!(
            super::mysql_metrics::METRIC_INTERPRETER_USEDTIME,
            instant.elapsed()
        );

        let collector = data_stream.collect::<Result<Vec<DataBlock>>>();
        let query_result = collector.await;
        query_result.map(|data| (data, Self::extra_info(context, instant)))
    }

    fn extra_info(context: &DatabendQueryContextRef, instant: Instant) -> String {
        let progress = context.get_progress_value();
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

    fn do_init(&mut self, database_name: &str) -> Result<()> {
        let init_query = format!("USE {};", database_name);
        let do_query = self.do_query(&init_query);

        match Self::build_runtime() {
            Err(error_code) => Err(error_code),
            Ok(runtime) => match runtime.block_on(do_query) {
                Ok(_) => Ok(()),
                Err(error_code) => Err(error_code),
            },
        }
    }

    fn build_runtime() -> Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|tokio_error| ErrorCode::TokioError(format!("{}", tokio_error)))
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
            version: crate::configs::DATABEND_COMMIT_VERSION.to_string(),
            client_addr,
        }
    }
}
