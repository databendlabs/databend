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

//use std::net::SocketAddr;
//use std::sync::Arc;
//
use std::fs::File;
use std::io::Read;

use common_exception::Result;
use common_runtime::tokio;

use crate::api::HttpService;
use crate::clusters::ClusterDiscovery;
use crate::configs::Config;
use crate::servers::Server;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;
use crate::tests::tls_constants::TEST_TLS_CA_CERT;
use crate::tests::tls_constants::TEST_TLS_CLIENT_IDENTITY;
use crate::tests::tls_constants::TEST_TLS_CLIENT_PASSWORD;

// need to support local_addr, but axum_server do not have local_addr callback
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server() -> Result<()> {
    let mut conf = Config::default();

    conf.query.api_tls_server_key = TEST_SERVER_KEY.to_owned();
    conf.query.api_tls_server_cert = TEST_SERVER_CERT.to_owned();

    let addr_str = "127.0.0.1:30001";
    let cluster = ClusterDiscovery::create_global(conf.clone())?;
    let mut srv = HttpService::create(conf.clone(), cluster.clone());
    let listening = srv.start(addr_str.parse()?).await?;
    let port = listening.port();

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/health", TEST_CN_NAME, port);

    // load cert
    let mut buf = Vec::new();
    File::open(TEST_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();

    // kick off
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();
    let resp = client.get(url).send().await;
    assert!(resp.is_ok());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!("/v1/health", resp.url().path());

    Ok(())
}

// client cannot communicate with server without ca certificate
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server_failed_case_1() -> Result<()> {
    let mut conf = Config::default();

    conf.query.api_tls_server_key = TEST_SERVER_KEY.to_owned();
    conf.query.api_tls_server_cert = TEST_SERVER_CERT.to_owned();

    let addr_str = "127.0.0.1:30010";
    let cluster = ClusterDiscovery::create_global(conf.clone())?;
    let mut srv = HttpService::create(conf.clone(), cluster.clone());
    let listening = srv.start(addr_str.parse()?).await?;
    let port = listening.port();

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/health", TEST_CN_NAME, port);
    // kick off
    let client = reqwest::Client::builder().build().unwrap();
    let resp = client.get(url).send().await;
    assert!(resp.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server_mutual_tls() -> Result<()> {
    use crate::tests::tls_constants::TEST_TLS_SERVER_CERT;
    use crate::tests::tls_constants::TEST_TLS_SERVER_KEY;

    let mut conf = Config::default();

    conf.query.api_tls_server_key = TEST_TLS_SERVER_KEY.to_owned();
    conf.query.api_tls_server_cert = TEST_TLS_SERVER_CERT.to_owned();
    conf.query.api_tls_server_root_ca_cert = TEST_TLS_CA_CERT.to_owned();

    let addr_str = "127.0.0.1:30011";
    let cluster = ClusterDiscovery::create_global(conf.clone())?;
    let mut srv = HttpService::create(conf.clone(), cluster.clone());
    let listening = srv.start(addr_str.parse()?).await?;
    let port = listening.port();

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/health", TEST_CN_NAME, port);

    // get identity
    let mut buf = Vec::new();
    File::open(TEST_TLS_CLIENT_IDENTITY)?.read_to_end(&mut buf)?;
    let pkcs12 = reqwest::Identity::from_pkcs12_der(&buf, TEST_TLS_CLIENT_PASSWORD).unwrap();
    let mut buf = Vec::new();
    File::open(TEST_TLS_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();
    // kick off
    let client = reqwest::Client::builder()
        .identity(pkcs12)
        .add_root_certificate(cert)
        .build()
        .expect("preconfigured rustls tls");
    let resp = client.get(url).send().await;
    assert!(resp.is_ok());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!("/v1/health", resp.url().path());
    Ok(())
}

// cannot connect with server unless it have CA signed identity
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server_mutual_tls_failed() -> Result<()> {
    use crate::tests::tls_constants::TEST_TLS_SERVER_CERT;
    use crate::tests::tls_constants::TEST_TLS_SERVER_KEY;

    let mut conf = Config::default();

    conf.query.api_tls_server_key = TEST_TLS_SERVER_KEY.to_owned();
    conf.query.api_tls_server_cert = TEST_TLS_SERVER_CERT.to_owned();
    conf.query.api_tls_server_root_ca_cert = TEST_TLS_CA_CERT.to_owned();

    let addr_str = "127.0.0.1:30012";
    let cluster = ClusterDiscovery::create_global(conf.clone())?;
    let mut srv = HttpService::create(conf.clone(), cluster.clone());
    let listening = srv.start(addr_str.parse()?).await?;
    let port = listening.port();

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/health", TEST_CN_NAME, port);
    let mut buf = Vec::new();
    File::open(TEST_TLS_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();
    // kick off
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .expect("preconfigured rustls tls");
    let resp = client.get(url).send().await;
    assert!(resp.is_err());
    Ok(())
}
