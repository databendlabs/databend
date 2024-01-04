// Copyright 2021 Datafuse Labs
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

use std::net::TcpListener;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_query::servers::FlightSQLServer;
use databend_query::test_kits::ConfigBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_sql_server_port_used() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:0").unwrap();
    let local_socket = listener.local_addr().unwrap();

    let mut srv = FlightSQLServer {
        config: ConfigBuilder::create().build(),
        abort_notify: Arc::new(Default::default()),
    };

    let r = srv.start_with_incoming(local_socket).await;

    assert!(r.is_err());
    Ok(())
}
