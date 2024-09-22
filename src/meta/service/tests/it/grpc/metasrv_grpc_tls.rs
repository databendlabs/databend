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

use std::time::Duration;

use databend_common_grpc::RpcClientTlsConfig;
use databend_common_meta_api::SchemaApi;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::start_metasrv_with_context;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_tls_server() -> anyhow::Result<()> {
    let mut tc = MetaSrvTestContext::new(0);

    tc.config.grpc_tls_server_key = TEST_SERVER_KEY.to_owned();
    tc.config.grpc_tls_server_cert = TEST_SERVER_CERT.to_owned();

    let r = start_metasrv_with_context(&mut tc).await;
    assert!(r.is_ok());

    let addr = tc.config.grpc_api_address.clone();

    let tls_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: TEST_CA_CERT.to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    };

    let client = MetaGrpcClient::try_create(
        vec![addr],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        Some(tls_conf),
    )?;

    let r = client
        .get_table(("do not care", "do not care", "do not care").into())
        .await;
    assert!(r.is_err());

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_tls_server_config_failure() -> anyhow::Result<()> {
    let mut tc = MetaSrvTestContext::new(0);

    tc.config.grpc_tls_server_key = "../tests/data/certs/not_exist.key".to_owned();
    tc.config.grpc_tls_server_cert = "../tests/data/certs/not_exist.pem".to_owned();

    let r = start_metasrv_with_context(&mut tc).await;
    assert!(r.is_err());
    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_tls_client_config_failure() -> anyhow::Result<()> {
    let tls_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: "../tests/data/certs/not_exist.pem".to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    };

    let r = MetaGrpcClient::try_create(
        vec!["addr".to_string()],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        Some(tls_conf),
    )
    .unwrap();

    let c = r.get_kv("foo").await;
    assert!(c.is_err(), "expect error: {:?}", c);

    let e = c.unwrap_err();

    if let MetaError::ClientError(MetaClientError::NetworkError(
        MetaNetworkError::TLSConfigError(any_err),
    )) = e
    {
        let _ = any_err;
    } else {
        unreachable!("expect tls error but: {:?}", e);
    }

    Ok(())
}
