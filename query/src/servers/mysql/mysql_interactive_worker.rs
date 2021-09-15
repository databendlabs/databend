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
use tokio_stream::StreamExt;

use crate::interpreters::InterpreterFactory;
use crate::servers::mysql::writers::DFInitResultWriter;
use crate::servers::mysql::writers::DFQueryResultWriter;
use crate::sessions::DatafuseQueryContextRef;
use crate::sessions::SessionRef;
use crate::sql::DfHint;
use crate::sql::PlanParser;

struct InteractiveWorkerBase<W: std::io::Write> {
    session: SessionRef,
    generic_hold: PhantomData<W>,
}

pub struct InteractiveWorker<W: std::io::Write> {
    session: SessionRef,
    base: InteractiveWorkerBase<W>,
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
                let blocks = runtime.block_on(self.base.do_query(query));

                if let Err(cause) = writer.write(blocks) {
                    let new_error = cause.add_message(query);
                    return Err(new_error);
                }

                Ok(())
            },
            Err(error) => writer.write(Err(error)),
        }
        //
        // histogram!(
        //     super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
        //     start.elapsed()
        // );
        //
        // Ok(())
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
    fn do_prepare(&mut self, _: &str, writer: StatementMetaWriter<'_, W>) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Prepare is not support in DataFuse.".as_bytes(),
        )?;
        Ok(())
    }

    fn do_execute(&mut self, _: u32, _: ParamParser<'_>, writer: QueryResultWriter<'_, W>) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Execute is not support in DataFuse.".as_bytes(),
        )?;
        Ok(())
    }

    fn do_close(&mut self, _: u32) {}

    async fn do_query(&mut self, query: &str) -> Result<Vec<DataBlock>> {
        log::debug!("{}", query);

        let context = self.session.create_context().await?;
        context.attach_query_str(query);

        let query_parser = PlanParser::create(context.clone());
        let (plan, hints) = query_parser.build_with_hint_from_sql(query);

        let instant = Instant::now();
        let interpreter = InterpreterFactory::get(context.clone(), plan?)?;
        let data_stream = interpreter.execute().await?;
        histogram!(super::mysql_metrics::METRIC_INTERPRETER_USEDTIME, instant.elapsed());

        match data_stream.collect::<Result<Vec<DataBlock>>>().await {
            Ok(blocks) => Ok(blocks),
            Err(cause) => match hints.iter().find(|v| v.error_code.is_some()) {
                None => Err(cause),
                Some(cause_hint) => match cause_hint.error_code {
                    None => Err(cause),
                    Some(code) if code == cause.code() => Ok(vec![DataBlock::empty()]),
                    Some(code) => {
                        let actual_code = cause.code();
                        Err(cause.add_message(format!(
                            "Expected server error code: {} but got: {}.",
                            code, actual_code
                        )))
                    },
                },
            },
        }
    }

    fn do_init(&mut self, database_name: &str) -> Result<()> {
        // self.do_query(&format!("USE {};", database_name))?;
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
        InteractiveWorker::<W> {
            session: session.clone(),
            base: InteractiveWorkerBase::<W> {
                session,
                generic_hold: PhantomData::default(),
            },
        }
    }
}
