use std::time::Instant;

use metrics::histogram;
use clickhouse_srv::connection::Connection;
use clickhouse_srv::errors::ServerError;
use clickhouse_srv::CHContext;
use clickhouse_srv::ClickHouseSession;
use common_datavalues::prelude::Arc;
use common_exception::ErrorCode;
use common_runtime::tokio::sync::mpsc;
use common_runtime::tokio::time::interval;
use common_runtime::tokio::time::Duration;
use futures::StreamExt;
use tokio_stream::wrappers::IntervalStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::servers::clickhouse::clickhouse_handler;
use crate::servers::clickhouse::interactive_worker_base::InteractiveWorkerBase;
use crate::servers::clickhouse::writers::QueryWriter;
use crate::servers::clickhouse::ClickHouseStream;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionRef;
use crate::sql::PlanParser;

pub struct InteractiveWorker {
    session: SessionRef,
}

impl InteractiveWorker {
    pub fn create(session: SessionRef) -> Arc<InteractiveWorker> {
        Arc::new(InteractiveWorker { session })
    }
}

#[async_trait::async_trait]
impl ClickHouseSession for InteractiveWorker {
    async fn execute_query(
        &self,
        ctx: &mut CHContext,
        conn: &mut Connection,
    ) -> clickhouse_srv::errors::Result<()> {
        let start = Instant::now();

        let context = self.session.create_context();
        let mut query_writer = QueryWriter::create(ctx.client_revision, conn, context.clone());

        let get_query_result = InteractiveWorkerBase::do_query(&ctx.state.query, context);
        query_writer.write(get_query_result.await).await?;

        histogram!(
            super::clickhouse_metrics::METRIC_CLICKHOUSE_PROCESSOR_REQUEST_DURATION,
            start.elapsed()
        );
        Ok(())
    }

    // TODO: remove it
    fn dbms_name(&self) -> &str {
        "datafuse"
    }

    // TODO: remove it
    fn server_display_name(&self) -> &str {
        "datafuse"
    }

    // TODO: remove it
    fn dbms_version_major(&self) -> u64 {
        2021
    }

    // TODO: remove it
    fn dbms_version_minor(&self) -> u64 {
        5
    }

    // TODO: remove it
    fn dbms_version_patch(&self) -> u64 {
        0
    }

    // TODO: remove it
    fn timezone(&self) -> &str {
        "UTC"
    }

    // TODO: remove it
    // the MIN_SERVER_REVISION for suggestions is 54406
    fn dbms_tcp_protocol_version(&self) -> u64 {
        54405
    }

    // TODO: remove it
    fn get_progress(&self) -> clickhouse_srv::types::Progress {
        unimplemented!()
    }
}
