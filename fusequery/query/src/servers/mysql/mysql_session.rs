use std::io;
use std::sync::Arc;
use std::time::Instant;

use futures::TryFutureExt;
use msql_srv::{ErrorKind, InitWriter, MysqlShim, ParamParser, QueryResultWriter, StatementMetaWriter};
use tokio_stream::StreamExt;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;

use crate::clusters::ClusterRef;
use crate::interpreters::{IInterpreter, InterpreterFactory};
use crate::servers::mysql::endpoints::{MySQLOnInitEndpoint, MySQLOnQueryEndpoint, IMySQLEndpoint};
use crate::sessions::SessionManagerRef;
use crate::sql::PlanParser;
use std::io::Error;
use metrics::histogram;

pub struct Session {
    cluster: ClusterRef,
    session_manager: SessionManagerRef,
    current_database: Arc<Mutex<String>>,
}

impl Session {
    pub fn create(cluster: ClusterRef, session_manager: SessionManagerRef) -> Self {
        Session {
            cluster,
            session_manager,
            current_database: Arc::new(Mutex::new(String::from("default"))),
        }
    }
}

impl<W: io::Write> MysqlShim<W> for Session {
    type Error = ErrorCode;

    fn on_prepare(&mut self, _: &str, writer: StatementMetaWriter<W>) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Prepare is not support in DataFuse.".as_bytes(),
        )?;

        Ok(())
    }

    fn on_execute(&mut self, _: u32, _: ParamParser, writer: QueryResultWriter<W>) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Execute is not support in DataFuse.".as_bytes(),
        )?;

        Ok(())
    }

    fn on_close(&mut self, _: u32) {
        // unimplemented!()
    }

    fn on_query(&mut self, query: &str, writer: QueryResultWriter<W>) -> Result<()> {
        log::debug!("{}", query);
        let cluster = self.cluster.clone();
        let database = self.current_database.lock().clone();
        let session_manager = self.session_manager.clone();

        MySQLOnQueryEndpoint::on_action(writer, move || {
            let start = Instant::now();

            let context = session_manager
                .try_create_context()?
                .with_cluster(cluster.clone())?;

            context.set_current_database(database.clone());

            let query_plan = PlanParser::create(context.clone()).build_from_sql(query)?;
            let query_interpreter = InterpreterFactory::get(context.clone(), query_plan)?;

            let received_data = futures::executor::block_on(query_interpreter
                .execute()
                .and_then(|stream| stream.collect::<Result<Vec<DataBlock>>>()));

            histogram!(
                super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
                start.elapsed()
            );

            session_manager.try_remove_context(context);
            received_data
        })
    }

    fn on_init(&mut self, database_name: &str, writer: InitWriter<W>) -> Result<()> {
        log::debug!("Use `{}` for MySQLHandler", database_name);
        let current_database = self.current_database.clone();

        MySQLOnInitEndpoint::on_action(writer, move || -> Result<()> {
            // TODO: test database is exists.

            *current_database.lock() = database_name.to_string();

            Ok(())
        })
    }
}
