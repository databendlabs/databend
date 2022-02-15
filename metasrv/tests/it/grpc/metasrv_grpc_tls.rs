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

use common_base::tokio;
use common_exception::ErrorCode;
use common_grpc::RpcClientTlsConfig;
use common_meta_api::MetaApi;
use common_meta_grpc::MetaGrpcClient;
use pretty_assertions::assert_eq;

use crate::init_meta_ut;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::start_metasrv_with_context;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_tls_server() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

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

    let client =
        MetaGrpcClient::try_create(addr.as_str(), "root", "xxx", None, Some(tls_conf)).await?;

    let r = client
        .get_table(("do not care", "do not care", "do not care").into())
        .await;
    assert!(r.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_tls_server_config_failure() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let mut tc = MetaSrvTestContext::new(0);

    tc.config.grpc_tls_server_key = "../tests/data/certs/not_exist.key".to_owned();
    tc.config.grpc_tls_server_cert = "../tests/data/certs/not_exist.pem".to_owned();

    let r = start_metasrv_with_context(&mut tc).await;
    assert!(r.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_tls_client_config_failure() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tls_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: "../tests/data/certs/not_exist.pem".to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    };

    let r = MetaGrpcClient::try_create("addr", "root", "xxx", None, Some(tls_conf))
        .await
        .unwrap();

    let c = r.make_client().await;
    assert!(c.is_err());

    if let Err(e) = c {
        let e = ErrorCode::from(e);
        assert_eq!(e.code(), ErrorCode::TLSConfigurationFailure("").code());
    }
    Ok(())
}
