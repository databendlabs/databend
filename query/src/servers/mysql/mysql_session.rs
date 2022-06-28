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

use common_base::base::tokio::net::TcpStream;
use common_base::base::Runtime;
use common_base::base::Thread;
use common_base::base::TrySpawn;
use common_exception::Result;
use opensrv_mysql::AsyncMysqlIntermediary;
use opensrv_mysql::IntermediaryOptions;

use crate::servers::mysql::mysql_interactive_worker::InteractiveWorker;
use crate::sessions::SessionRef;

pub struct MySQLConnection;

impl MySQLConnection {
    pub fn run_on_stream(session: SessionRef, stream: TcpStream) -> Result<()> {
        let query_executor =
            Runtime::with_worker_threads(1, Some("mysql-query-executor".to_string()))?;
        Thread::spawn(move || {
            let join_handle = query_executor.spawn(async move {
                let client_addr = stream.peer_addr().unwrap().to_string();
                let interactive_worker = InteractiveWorker::create(session, client_addr);
                let opts = IntermediaryOptions {
                    process_use_statement_on_query: true,
                };
                AsyncMysqlIntermediary::run_with_options(interactive_worker, stream, &opts).await
            });
            let _ = futures::executor::block_on(join_handle);
        });
        Ok(())
    }
}
