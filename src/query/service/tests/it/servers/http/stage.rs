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

use std::path::Path;

use databend_common_meta_app::principal::StageInfo;
use databend_query::servers::http::v1::ExecuteStateKind;
use databend_query::servers::http::v1::UploadToStageResponse;
use databend_query::test_kits::TestFixture;
use futures_util::stream::poll_fn;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use http::header;
use poem::Body;
use poem::Endpoint;
use poem::Request;
use poem::Response;
use poem::Route;

use super::http_query_handlers::create_endpoint;
use super::http_query_handlers::post_sql_to_endpoint_new_session;

const UPLOADER: &str = "http_stage_uploader";
const UPLOAD_CONTENT: &str = "stage upload test\n";
const FILE_NAME: &str = "fixture.txt";
const BOUNDARY: &str = "stage-upload-test";

async fn admin_sql(ep: &Route, sql: &str) -> anyhow::Result<()> {
    let (status, result) =
        post_sql_to_endpoint_new_session(ep, sql, 5, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK, "{sql}: {:?}", result.error);
    assert!(result.error.is_none(), "{sql}: {:?}", result.error);
    assert_eq!(result.state, ExecuteStateKind::Succeeded, "{sql}");
    Ok(())
}

async fn upload(
    ep: &Route,
    stage_header: &str,
    stage_name: &str,
    body: impl Into<Body>,
) -> poem::Result<Response> {
    let req = Request::builder()
        .uri("/v1/upload_to_stage".parse().unwrap())
        .method(Method::PUT)
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={BOUNDARY}"),
        )
        .header(stage_header, stage_name)
        .typed_header(headers::Authorization::basic(UPLOADER, "test-password"))
        .body(body);
    ep.call(req).await
}

#[tokio::test(flavor = "current_thread")]
async fn test_upload_to_stage_write_access() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    for sql in [
        "CREATE USER http_stage_uploader IDENTIFIED BY 'test-password'",
        "CREATE ROLE http_stage_upload_role",
        "GRANT ROLE http_stage_upload_role TO USER http_stage_uploader",
        "CREATE STAGE http_stage_grants",
        "CREATE STAGE http_stage_owned",
        "GRANT OWNERSHIP ON STAGE http_stage_owned TO ROLE http_stage_upload_role",
    ] {
        admin_sql(&ep, sql).await?;
    }

    let stage = StageInfo::new_internal_stage("http_stage_grants");
    let prefix = stage.stage_prefix();
    let stage_dir = Path::new(fixture.storage_root()).join(prefix.trim_matches('/'));
    // A denied upload must not consume the request body or create any file.
    for grant in [
        None,
        Some("GRANT READ ON STAGE http_stage_grants TO ROLE http_stage_upload_role"),
    ] {
        if let Some(grant) = grant {
            admin_sql(&ep, grant).await?;
        }
        let body = Body::from_bytes_stream::<_, &'static [u8], std::io::Error>(poll_fn(|_| {
            panic!("a denied upload must not read the request body")
        }));
        let response = upload(&ep, "x-databend-stage-name", &stage.stage_name, body).await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "grant: {grant:?}");
        assert!(
            tokio::fs::read_dir(&stage_dir)
                .await?
                .next_entry()
                .await?
                .is_none()
        );
    }

    admin_sql(
        &ep,
        "GRANT WRITE ON STAGE http_stage_grants TO ROLE http_stage_upload_role",
    )
    .await?;
    for (stage_header, stage_name, target) in [
        ("x-databend-stage-name", stage.stage_name.as_str(), &stage),
        (
            "stage_name",
            "http_stage_owned",
            &StageInfo::new_internal_stage("http_stage_owned"),
        ),
        (
            "x-databend-stage-name",
            "~",
            &StageInfo::new_user_stage(UPLOADER),
        ),
    ] {
        let body = format!(
            "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"upload\"; filename=\"{FILE_NAME}\"\r\n\r\n{UPLOAD_CONTENT}\r\n--{BOUNDARY}--\r\n"
        );
        let response = upload(&ep, stage_header, stage_name, body).await?;
        assert_eq!(response.status(), StatusCode::OK, "stage: {stage_name}");
        let response: UploadToStageResponse = response.into_body().into_json().await?;
        assert_eq!(response.stage_name, stage_name);
        assert_eq!(response.state, "SUCCESS");
        assert_eq!(response.files, [FILE_NAME]);

        let path = Path::new(fixture.storage_root())
            .join(target.stage_prefix().trim_matches('/'))
            .join(FILE_NAME);
        assert_eq!(tokio::fs::read(path).await?, UPLOAD_CONTENT.as_bytes());
    }
    Ok(())
}
