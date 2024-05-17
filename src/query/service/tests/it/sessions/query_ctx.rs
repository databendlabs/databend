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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_query::sessions::short_sql;
use databend_query::sessions::TableContext;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_s3() -> Result<()> {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bucket"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let mut conf = ConfigBuilder::create().config();
    conf.storage.params = StorageParams::S3(StorageS3Config {
        region: "us-east-2".to_string(),
        endpoint_url: mock_server.uri(),
        bucket: "bucket".to_string(),
        access_key_id: "access_key_id".to_string(),
        secret_access_key: "secret_access_key".to_string(),
        disable_credential_loader: true,
        ..Default::default()
    });
    let fixture = TestFixture::setup_with_config(&conf).await?;
    let ctx = fixture.new_query_ctx().await?;

    let _ = ctx.get_data_operator()?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_fs() -> Result<()> {
    let mut conf = ConfigBuilder::create().config();
    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        root: "/tmp".to_string(),
    });
    let fixture = TestFixture::setup_with_config(&conf).await?;
    let ctx = fixture.new_query_ctx().await?;
    let _ = ctx.get_data_operator()?;

    Ok(())
}

#[test]
fn test_short_sql() {
    // Test case 1: SQL query shorter than 10KB
    let sql1 = "SELECT * FROM users WHERE id = 1;".to_string();
    assert_eq!(short_sql(sql1.clone()), sql1);

    // Test case 2: SQL query longer than 10KB and starts with "INSERT"
    let long_sql = "INSERT INTO users (id, name, email) VALUES ".to_string()
        + &"(1, 'John Doe', 'john@example.com'), ".repeat(1000);
    let expected_result = long_sql.as_bytes()[..10240].to_vec();
    let expected_result = String::from_utf8(expected_result).unwrap() + "...";
    assert_eq!(short_sql(long_sql), expected_result);

    // Test case 3: SQL query longer than 10KB but does not start with "INSERT"
    let long_sql = "SELECT * FROM users WHERE ".to_string() + &"id = 1 OR ".repeat(1000);
    assert_eq!(short_sql(long_sql.clone()), long_sql);

    // Test case 4: Empty SQL query
    let empty_sql = "".to_string();
    assert_eq!(short_sql(empty_sql.clone()), empty_sql);

    // Test case 5: SQL query with leading whitespace
    let sql_with_whitespace = "   SELECT * FROM users;".to_string();
    assert_eq!(short_sql(sql_with_whitespace.clone()), sql_with_whitespace);
}
