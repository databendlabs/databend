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

use std::sync::Arc;
use std::time::Instant;

use common_clickhouse_srv::connection::Connection;
use common_clickhouse_srv::CHContext;
use common_clickhouse_srv::ClickHouseSession;
use metrics::histogram;

use crate::servers::clickhouse::interactive_worker_base::InteractiveWorkerBase;
use crate::servers::clickhouse::writers::to_clickhouse_err;
use crate::servers::clickhouse::writers::QueryWriter;
use crate::sessions::SessionRef;
use crate::users::CertifiedInfo;

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
    ) -> common_clickhouse_srv::errors::Result<()> {
        let start = Instant::now();

        let mut query_writer = QueryWriter::create(ctx.client_revision, conn);

        let session = self.session.clone();
        let get_query_result = InteractiveWorkerBase::do_query(ctx, session);
        if let Err(cause) = query_writer.write(get_query_result.await).await {
            let new_error = cause.add_message(&ctx.state.query);
            return Err(to_clickhouse_err(new_error));
        }

        histogram!(
            super::clickhouse_metrics::METRIC_CLICKHOUSE_PROCESSOR_REQUEST_DURATION,
            start.elapsed()
        );
        Ok(())
    }

    // TODO: remove it
    fn dbms_name(&self) -> &str {
        "databend"
    }

    // TODO: remove it
    fn server_display_name(&self) -> &str {
        "databend"
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

    fn authenticate(&self, user: &str, password: &[u8], client_addr: &str) -> bool {
        let info = CertifiedInfo::create(user, password, client_addr);

        let user_manager = self.session.get_user_manager();
        // TODO: push async up to clickhouse server lib
        futures::executor::block_on(async move {
            match user_manager.auth_user(info).await {
                Ok(res) => res,
                Err(failure) => {
                    log::error!(
                        "ClickHouse handler authenticate failed, \
                        user: {}, \
                        client_address: {}, \
                        cause: {:?}",
                        user,
                        client_addr,
                        failure
                    );
                    false
                }
            }
        })
    }

    // TODO: remove it
    fn get_progress(&self) -> common_clickhouse_srv::types::Progress {
        unimplemented!()
    }
}
