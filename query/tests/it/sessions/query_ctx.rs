// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio;
use common_exception::Result;
use common_io::prelude::StorageFsConfig;
use common_io::prelude::StorageParams;
use common_io::prelude::StorageS3Config;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_s3() -> Result<()> {
    let mock_server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/bucket/.opendal"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let mut conf = crate::tests::ConfigBuilder::create().config();

    conf.storage.params = StorageParams::S3(StorageS3Config {
        region: "us-east-2".to_string(),
        endpoint_url: mock_server.uri(),
        bucket: "bucket".to_string(),
        ..Default::default()
    });

    let qctx = crate::tests::create_query_context_with_config(conf, None).await?;

    let _ = qctx.get_storage_operator()?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_fs() -> Result<()> {
    let mut conf = crate::tests::ConfigBuilder::create().config();

    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        root: "/tmp".to_string(),
    });

    let qctx = crate::tests::create_query_context_with_config(conf, None).await?;

    let _ = qctx.get_storage_operator()?;

    Ok(())
}
