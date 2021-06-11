use futures::future::{Abortable, Aborted, AbortHandle};
use futures::{TryFutureExt};
use msql_srv::{ErrorKind, InitWriter, MysqlShim, ParamParser, QueryResultWriter, StatementMetaWriter};

use common_exception::ErrorCode;
use common_datablocks::DataBlock;
use common_exception::Result;

use crate::interpreters::{IInterpreter, InterpreterFactory};
use crate::servers::mysql::endpoints::{MySQLOnInitEndpoint, MySQLOnQueryEndpoint, IMySQLEndpoint};
use crate::sessions::{ISession, SessionStatus};
use crate::sql::PlanParser;
use std::sync::Arc;
use std::time::Instant;
use common_streams::AbortStream;
use tokio_stream::StreamExt;
use metrics::histogram;


pub struct InteractiveWorker {
    session: Arc<Box<dyn ISession>>,
}

impl<W: std::io::Write> MysqlShim<W> for InteractiveWorker {
    type Error = ErrorCode;

    fn on_prepare(&mut self, _: &str, writer: StatementMetaWriter<W>) -> Result<()> {
        writer.error(ErrorKind::ER_UNKNOWN_ERROR, "Prepare is not support in DataFuse.".as_bytes())?;

        Ok(())
    }

    fn on_execute(&mut self, _: u32, _: ParamParser, writer: QueryResultWriter<W>) -> Result<()> {
        writer.error(ErrorKind::ER_UNKNOWN_ERROR, "Execute is not support in DataFuse.".as_bytes())?;

        Ok(())
    }

    fn on_close(&mut self, _: u32) {
        // unimplemented!()
    }

    fn on_query(&mut self, query: &str, writer: QueryResultWriter<W>) -> Result<()> {
        log::debug!("{}", query);

        MySQLOnQueryEndpoint::on_action(writer, move || {
            let start = Instant::now();
            let context = self.session.try_create_context()?;

            self.session.get_status().lock().enter_parser(query);
            let query_plan = PlanParser::create(context.clone()).build_from_sql(query)?;

            self.session.get_status().lock().enter_interpreter(&query_plan);
            let query_interpreter = InterpreterFactory::get(context.clone(), query_plan)?;
            let stream = futures::executor::block_on(query_interpreter.execute())?;

            let (abort_handle, stream) = AbortStream::try_create(stream)?;
            self.session.get_status().lock().enter_fetch_data(abort_handle);
            let received_data = futures::executor::block_on(stream.collect::<Result<Vec<DataBlock>>>());

            histogram!(
                super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
                start.elapsed()
            );

            received_data
        })
    }

    fn on_init(&mut self, database_name: &str, writer: InitWriter<W>) -> Result<()> {
        log::debug!("Use `{}` for MySQLHandler", database_name);
        // let current_database = self.current_database.clone();

        MySQLOnInitEndpoint::on_action(writer, move || -> Result<()> {
            // TODO: test database is exists.

            // *current_database.lock() = database_name.to_string();

            Ok(())
        })
    }
}

impl InteractiveWorker {
    pub fn create(session: Arc<Box<dyn ISession>>) -> InteractiveWorker {
        InteractiveWorker { session }
    }
}
