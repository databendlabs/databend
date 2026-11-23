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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

async fn query_error(fixture: &TestFixture, sql: &str) -> ErrorCode {
    let result = match fixture.execute_query(sql).await {
        Ok(stream) => stream.try_collect::<Vec<DataBlock>>().await.map(|_| ()),
        Err(err) => Err(err),
    };
    result.expect_err(sql)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stored_locations_respect_allow_insecure() -> Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.storage.allow_insecure = false;
    let fixture = TestFixture::setup_with_config(&config).await?;
    let db = fixture.default_db_name();
    fixture
        .execute_command(&format!("create database {db}"))
        .await?;

    let dir = tempfile::tempdir()?;
    let url = format!("fs://{}/", dir.path().display());

    for sql in [
        format!("create stage insecure_fs_stage url = '{url}'"),
        format!(
            "create stage insecure_http_stage url = 's3://bucket/dir/' connection = (endpoint_url = 'http://127.0.0.1:9000' access_key_id = 'a' secret_access_key = 'b')"
        ),
        format!("create table {db}.insecure_fs_table(a int) '{url}'"),
        format!("create temporary table {db}.insecure_fs_tmp(a int) '{url}'"),
    ] {
        let err = fixture.execute_command(&sql).await.expect_err(sql.as_str());
        assert_eq!(err.code(), ErrorCode::STORAGE_INSECURE, "{sql}: {err}");
    }

    fixture
        .execute_command("create stage internal_stage")
        .await?;
    let sql = format!("alter stage internal_stage set url = '{url}'");
    let err = fixture.execute_command(&sql).await.expect_err(sql.as_str());
    assert_eq!(err.code(), ErrorCode::STORAGE_INSECURE, "{sql}: {err}");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_user_name_must_be_a_single_path_component() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    for name in ["", ".", "..", "../..", "alice/", "/"] {
        let sql = format!("create user '{name}' identified by 'password'");
        let err = fixture.execute_command(&sql).await.expect_err(sql.as_str());
        assert_eq!(err.code(), ErrorCode::ILLEGAL_USER, "{sql}: {err}");
    }

    fixture
        .execute_command("create user 'user@example.com' identified by 'password'")
        .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_file_rejects_parent_components() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command("create stage read_file_stage")
        .await?;
    fixture
        .execute_command("create table read_file_paths(stage_name string, path string)")
        .await?;
    fixture
        .execute_command(
            "insert into read_file_paths values ('@read_file_stage', '../x'), ('@~', 'a/../../x')",
        )
        .await?;

    for sql in [
        "select read_file('@~/../x')",
        "select read_file('@read_file_stage/a/../../x')",
        "select read_file('@~', '../x')",
        "select read_file(stage_name || '/' || path) from read_file_paths",
        "select read_file('@read_file_stage', path) from read_file_paths",
        "select read_file(stage_name, '../x') from read_file_paths",
        "select read_file(stage_name, path) from read_file_paths",
    ] {
        let err = query_error(&fixture, sql).await;
        assert!(
            err.message()
                .contains("stage path traversal is not allowed"),
            "{sql}: {err}"
        );
    }

    Ok(())
}
