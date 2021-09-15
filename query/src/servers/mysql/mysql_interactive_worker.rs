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

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio;
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
use crate::servers::server::mock::get_mock_user;
use crate::sessions::DatafuseQueryContextRef;
use crate::sessions::SessionRef;
use crate::sql::DfHint;
use crate::sql::PlanParser;

struct InteractiveWorkerBase<W: std::io::Write>(PhantomData<W>);

pub struct InteractiveWorker<W: std::io::Write> {
    base: InteractiveWorkerBase<W>,
    session: SessionRef,
    version: String,
    salt: [u8; 20],
}

impl<W: std::io::Write> MysqlShim<W> for InteractiveWorker<W> {
    type Error = ErrorCode;

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

        self.base
            .do_prepare(query, writer, self.session.create_context())
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

        self.base
            .do_execute(id, param, writer, self.session.create_context())
    }

    fn on_close(&mut self, id: u32) {
        self.base.do_close(id, self.session.create_context());
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

        let start = Instant::now();
        let context = self.session.create_context();

        context.attach_query_str(query);
        if let Err(cause) =
            DFQueryResultWriter::create(writer).write(self.base.do_query(query, context))
        {
            let new_error = cause.add_message(query);
            return Err(new_error);
        };

        histogram!(
            super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
            start.elapsed()
        );

        Ok(())
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

        let context = self.session.create_context();
        DFInitResultWriter::create(writer).write(self.base.do_init(database_name, context))
    }

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
        let user = String::from_utf8_lossy(username);

        if let Ok(user) = get_mock_user(&user) {
            let encode_password = match auth_plugin {
                "mysql_native_password" => {
                    if auth_data.is_empty() {
                        vec![]
                    } else {
                        // SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
                        let mut m = sha1::Sha1::new();
                        m.update(salt);
                        m.update(&user.password);

                        let result = m.digest().bytes();
                        if auth_data.len() != result.len() {
                            return false;
                        }
                        let mut s = Vec::with_capacity(result.len());
                        for i in 0..result.len() {
                            s.push(auth_data[i] ^ result[i]);
                        }
                        s
                    }
                }
                _ => auth_data.to_vec(),
            };
            return user.authenticate_user(encode_password);
        }

        false
    }
}

impl<W: std::io::Write> InteractiveWorkerBase<W> {
    fn do_prepare(
        &mut self,
        _: &str,
        writer: StatementMetaWriter<'_, W>,
        _: DatafuseQueryContextRef,
    ) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Prepare is not support in DataFuse.".as_bytes(),
        )?;
        Ok(())
    }

    fn do_execute(
        &mut self,
        _: u32,
        _: ParamParser<'_>,
        writer: QueryResultWriter<'_, W>,
        _: DatafuseQueryContextRef,
    ) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Execute is not support in DataFuse.".as_bytes(),
        )?;
        Ok(())
    }

    fn do_close(&mut self, _: u32, _: DatafuseQueryContextRef) {}

    fn do_query(
        &mut self,
        query: &str,
        context: DatafuseQueryContextRef,
    ) -> Result<Vec<DataBlock>> {
        log::debug!("{}", query);

        let runtime = Self::build_runtime()?;
        let (plan, hints) = PlanParser::create(context.clone()).build_with_hint_from_sql(query);

        let fetch_query_blocks = || -> Result<Vec<DataBlock>> {
            let start = Instant::now();
            let interpreter = InterpreterFactory::get(context.clone(), plan?)?;
            let name = interpreter.name().to_string();
            let data_stream = runtime.block_on(interpreter.execute())?;
            histogram!(
                super::mysql_metrics::METRIC_INTERPRETER_USEDTIME,
                start.elapsed(),
                "interpreter" => name
            );
            runtime.block_on(data_stream.collect::<Result<Vec<DataBlock>>>())
        };
        let blocks = fetch_query_blocks();
        match blocks {
            Ok(v) => Ok(v),
            Err(e) => {
                let hint = hints.iter().find(|v| v.error_code.is_some());
                if let Some(DfHint {
                    error_code: Some(code),
                    ..
                }) = hint
                {
                    if *code == e.code() {
                        Ok(vec![DataBlock::empty()])
                    } else {
                        let actual_code = e.code();
                        Err(e.add_message(format!(
                            "Expected server error code: {} but got: {}.",
                            code, actual_code
                        )))
                    }
                } else {
                    Err(e)
                }
            }
        }
    }

    fn do_init(&mut self, database_name: &str, context: DatafuseQueryContextRef) -> Result<()> {
        self.do_query(&format!("USE {};", database_name), context)?;
        Ok(())
    }

    fn build_runtime() -> Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|tokio_error| ErrorCode::TokioError(format!("{}", tokio_error)))
    }
}

impl<W: std::io::Write> InteractiveWorker<W> {
    pub fn create(session: SessionRef) -> InteractiveWorker<W> {
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

        let context = session.create_context();

        InteractiveWorker::<W> {
            session,
            base: InteractiveWorkerBase::<W>(PhantomData::<W>),
            salt: scramble,
            version: context.get_fuse_version(),
        }
    }
}
