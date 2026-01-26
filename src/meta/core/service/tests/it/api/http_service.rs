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

use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use databend_common_meta_runtime_api::RuntimeApi;
use databend_common_meta_runtime_api::TokioRuntime;
use databend_meta::meta_node::meta_worker::MetaWorker;
use databend_meta_admin::HttpService;
use databend_meta_admin::HttpServiceConfig;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

// TODO(zhihanz) add tls fail case
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_http_service_tls_server() -> anyhow::Result<()> {
    let addr_str = "127.0.0.1:30002";
    let tc = MetaSrvTestContext::new(0);

    let runtime = TokioRuntime::new_testing("meta-io-rt-ut");
    let mh = MetaWorker::create_meta_worker(tc.config.clone(), Arc::new(runtime)).await?;
    let mh = Arc::new(mh);

    let http_cfg = HttpServiceConfig {
        admin_api_address: addr_str.to_string(),
        tls: databend_meta::configs::TlsConfig {
            cert: TEST_SERVER_CERT.to_owned(),
            key: TEST_SERVER_KEY.to_owned(),
        },
        config_display: "test config".to_string(),
    };
    let mut srv = HttpService::create(http_cfg, mh);
    // test cert is issued for "localhost"
    let url = format!("https://{}:30002/v1/health", TEST_CN_NAME);

    // load cert
    let mut buf = Vec::new();
    File::open(TEST_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();

    srv.do_start().await.expect("HTTP: admin api error");
    // kick off
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();
    let resp = client.get(url).send().await;
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!("/v1/health", resp.url().path());
    Ok(())
}
