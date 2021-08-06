// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//use std::net::SocketAddr;
//use std::sync::Arc;
//
use std::fs::File;
use std::io::Read;

use common_exception::Result;
use common_runtime::tokio;

use crate::api::HttpService;
use crate::clusters::Cluster;
use crate::configs::Config;
use crate::servers::Server;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server() -> Result<()> {
    let mut conf = Config::default();

    conf.api_tls_server_key = "../../tests/data/certs/server.key".to_owned();
    conf.api_tls_server_cert = "../../tests/data/certs/server.pem".to_owned();

    let addr_str = "127.0.0.1:0";
    let cluster = Cluster::create_global(conf.clone())?;
    let mut srv = HttpService::create(conf.clone(), cluster.clone());
    let listening = srv.start(addr_str.parse()?).await?;
    let port = listening.port();

    // test cert is issued for "localhost"
    let url = format!("https://localhost:{}/v1/hello", port);

    // load cert
    let mut buf = Vec::new();
    File::open("../../tests/data/certs/ca.pem")?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();

    // kick off
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .danger_accept_invalid_hostnames(true)
        .build()
        .unwrap();
    let resp = client.get(url).send().await;
    assert!(resp.is_ok());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!("/v1/hello", resp.url().path());

    Ok(())
}
