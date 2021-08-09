// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

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
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionRef;
use crate::sql::DfHint;
use crate::sql::PlanParser;

struct InteractiveWorkerBase<W: std::io::Write>(PhantomData<W>);

pub struct InteractiveWorker<W: std::io::Write> {
    base: InteractiveWorkerBase<W>,
    session: SessionRef,
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

        context.attach_query_info(query);
        match DFQueryResultWriter::create(writer).write(self.base.do_query(query, context)) {
            Ok(_) => {}
            Err(e) => {
                let query_str = String::from(query);
                let new_error = e.add_message(query_str);
                return Err(new_error);
            }
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
}

impl<W: std::io::Write> InteractiveWorkerBase<W> {
    fn do_prepare(
        &mut self,
        _: &str,
        writer: StatementMetaWriter<'_, W>,
        _: FuseQueryContextRef,
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
        _: FuseQueryContextRef,
    ) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Execute is not support in DataFuse.".as_bytes(),
        )?;
        Ok(())
    }

    fn do_close(&mut self, _: u32, _: FuseQueryContextRef) {}

    fn do_query(&mut self, query: &str, context: FuseQueryContextRef) -> Result<Vec<DataBlock>> {
        log::debug!("{}", query);

        let runtime = Self::build_runtime()?;
        let (plan, hints) = PlanParser::create(context.clone()).build_with_hint_from_sql(query);

        let fetch_query_blocks = || -> Result<Vec<DataBlock>> {
            let interpreter = InterpreterFactory::get(context.clone(), plan?)?;
            let data_stream = runtime.block_on(interpreter.execute())?;

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

    fn do_init(&mut self, database_name: &str, context: FuseQueryContextRef) -> Result<()> {
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
        InteractiveWorker::<W> {
            session,
            base: InteractiveWorkerBase::<W>(PhantomData::<W>),
        }
    }
}
