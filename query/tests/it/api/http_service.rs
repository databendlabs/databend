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

use std::fs::File;
use std::io::Read;

use common_base::tokio;
use common_exception::Result;
use databend_query::api::HttpService;
use databend_query::servers::Server;

use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;
use crate::tests::tls_constants::TEST_TLS_CA_CERT;
use crate::tests::tls_constants::TEST_TLS_CLIENT_IDENTITY;
use crate::tests::tls_constants::TEST_TLS_CLIENT_PASSWORD;
use crate::tests::tls_constants::TEST_TLS_SERVER_CERT;
use crate::tests::tls_constants::TEST_TLS_SERVER_KEY;
use crate::tests::SessionManagerBuilder;

// need to support local_addr, but axum_server do not have local_addr callback
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server() -> Result<()> {
    let address_str = "127.0.0.1:30001";
    let mut srv = HttpService::create(
        SessionManagerBuilder::create()
            .api_tls_server_key(TEST_SERVER_KEY)
            .api_tls_server_cert(TEST_SERVER_CERT)
            .build()?,
    );

    let listening = srv.start(address_str.parse()?).await?;

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/health", TEST_CN_NAME, listening.port());

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

    assert!(resp.is_ok(), "{:?}", resp.err().unwrap());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!("/v1/health", resp.url().path());

    Ok(())
}

// client cannot communicate with server without ca certificate
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server_failed_case_1() -> Result<()> {
    let address = "127.0.0.1:30010";
    let mut http_service = HttpService::create(
        SessionManagerBuilder::create()
            .api_tls_server_key(TEST_SERVER_KEY)
            .api_tls_server_cert(TEST_SERVER_CERT)
            .build()?,
    );
    let listening = http_service.start(address.parse()?).await?;

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/health", TEST_CN_NAME, listening.port());
    // kick off
    let client = reqwest::Client::builder().build().unwrap();
    let resp = client.get(url).send().await;
    assert!(resp.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server_mutual_tls() -> Result<()> {
    let addr_str = "127.0.0.1:30011";
    let mut srv = HttpService::create(
        SessionManagerBuilder::create()
            .api_tls_server_key(TEST_TLS_SERVER_KEY)
            .api_tls_server_cert(TEST_TLS_SERVER_CERT)
            .api_tls_server_root_ca_cert(TEST_TLS_CA_CERT)
            .build()?,
    );
    let listening = srv.start(addr_str.parse()?).await?;

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/health", TEST_CN_NAME, listening.port());

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
    let addr_str = "127.0.0.1:30012";
    let mut srv = HttpService::create(
        SessionManagerBuilder::create()
            .api_tls_server_key(TEST_TLS_SERVER_KEY)
            .api_tls_server_cert(TEST_TLS_SERVER_CERT)
            .api_tls_server_root_ca_cert(TEST_TLS_CA_CERT)
            .build()?,
    );
    let listening = srv.start(addr_str.parse()?).await?;

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/health", TEST_CN_NAME, listening.port());
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
