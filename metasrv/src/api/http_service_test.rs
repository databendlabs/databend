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

use std::fs::File;
use std::io::Read;

use common_exception::Result;
use common_base::tokio;

use crate::api::HttpService;
use crate::configs::Config;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

// TODO(zhihanz) add tls fail case
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server() -> Result<()> {
    let mut conf = Config::empty();
    let addr_str = "127.0.0.1:0";

    conf.admin_tls_server_key = TEST_SERVER_KEY.to_owned();
    conf.admin_tls_server_cert = TEST_SERVER_CERT.to_owned();
    conf.admin_api_address = addr_str.to_owned();

    let mut srv = HttpService::create(conf);

    // test cert is issued for "localhost"
    let url = format!("https://{}:0/v1/health", TEST_CN_NAME);

    // load cert
    let mut buf = Vec::new();
    File::open(TEST_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();

    tokio::spawn(async move {
        srv.start().await.expect("HTTP: admin api error");
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
    });

    Ok(())
}
