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
use common_base::Stoppable;
use common_exception::Result;
use databend_meta::api::HttpService;
use databend_meta::configs::Config;
use databend_meta::meta_service::MetaNode;

use crate::init_meta_ut;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

// TODO(zhihanz) add tls fail case
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_http_service_tls_server() -> Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let mut conf = Config::empty();
    let addr_str = "127.0.0.1:30002";

    conf.admin_tls_server_key = TEST_SERVER_KEY.to_owned();
    conf.admin_tls_server_cert = TEST_SERVER_CERT.to_owned();
    conf.admin_api_address = addr_str.to_owned();
    let tc = MetaSrvTestContext::new(0);
    let meta_node = MetaNode::start(&tc.config.raft_config).await?;

    let mut srv = HttpService::create(conf, meta_node);
    // test cert is issued for "localhost"
    let url = format!("https://{}:30002/v1/health", TEST_CN_NAME);

    // load cert
    let mut buf = Vec::new();
    File::open(TEST_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();

    srv.start().await.expect("HTTP: admin api error");
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
