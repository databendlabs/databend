// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//use std::net::SocketAddr;
//use std::sync::Arc;
//
use common_exception::Result;
use common_runtime::tokio;
//use common_runtime::tokio::net::TcpListener;
//
//use crate::api::http::router::Router;
//use crate::api::HttpService;
//use crate::clusters::Cluster;
//use crate::clusters::ClusterRef;
//use crate::configs::Config;
//use crate::servers::Server;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server() -> Result<()> {
    //let mut conf = Config::default();
    //conf.rpc_tls_server_key = "../../tests/data/certs/server.key".to_owned();
    //conf.rpc_tls_server_cert = "../../tests/data/certs/server.pem".to_owned();

    //let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    //let address = listener.local_addr().unwrap();

    //let cluster = Cluster::create_global(conf.clone())?;
    //let listening = conf.http_api_address.parse::<std::net::SocketAddr>()?;
    //let mut srv = HttpService::create(conf.clone(), cluster.clone());
    //let listening = srv.start(address).await?;
    Ok(())
}
