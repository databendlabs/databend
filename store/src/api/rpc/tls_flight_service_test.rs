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

use common_exception::ErrorCode;
use common_flights::MetaApi;
use common_flights::RpcClientTlsConfig;
use common_flights::StoreClient;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::tests::service::new_test_context;
use crate::tests::start_store_server_with_context;

const TEST_CA_CERT: &'static str = "../tests/certs/ca.pem";
const TEST_SERVER_CERT: &'static str = "../tests/certs/server.pem";
const TEST_SERVER_KEY: &'static str = "../tests/certs/server.key";
const TEST_CN_NAME: &'static str = "localhost";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_tls() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let mut tc = new_test_context();

    tc.config.rpc_tls_server_key = TEST_SERVER_KEY.to_owned();
    tc.config.rpc_tls_server_cert = TEST_SERVER_CERT.to_owned();

    let r = start_store_server_with_context(&mut tc).await;
    assert!(r.is_ok());

    let addr = tc.config.flight_api_address.clone();

    let tls_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: TEST_CA_CERT.to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    };

    let mut client =
        StoreClient::with_tls_conf(addr.as_str(), "root", "xxx", Some(tls_conf)).await?;

    let r = client
        .get_table("do not care".to_owned(), "do not care".to_owned())
        .await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(e.code(), ErrorCode::UnknownDatabase("").code());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_tls_server_config_failure() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let mut tc = new_test_context();

    tc.config.rpc_tls_server_key = "../tests/data/certs/not_exist.key".to_owned();
    tc.config.rpc_tls_server_cert = "../tests/data/certs/not_exist.pem".to_owned();

    let r = start_store_server_with_context(&mut tc).await;
    assert!(r.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_tls_client_config_failure() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let tls_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: "../tests/data/certs/not_exist.pem".to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    };

    let r = StoreClient::with_tls_conf("addr", "root", "xxx", Some(tls_conf)).await;

    assert!(r.is_err());
    if let Err(e) = r {
        assert_eq!(e.code(), ErrorCode::TLSConfigurationFailure("").code());
    }
    Ok(())
}
