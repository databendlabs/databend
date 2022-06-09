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

use chrono::DateTime;
use chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_meta_types::StageFile;
use common_meta_types::StageType;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::http::StatusCode;
use poem::web::Json;
use poem::web::Multipart;
use poem::Request;
use serde::Deserialize;
use serde::Serialize;

use super::HttpQueryContext;
use crate::sessions::SessionType;
use crate::storages::stage::StageSource;

#[derive(Serialize, Deserialize, Debug)]
pub struct UploadToStageResponse {
    pub id: String,
    pub stage_name: String,
    pub state: String,
    pub files: Vec<String>,
}

#[poem::handler]
pub async fn upload_to_stage(
    ctx: &HttpQueryContext,
    req: &Request,
    mut multipart: Multipart,
) -> PoemResult<Json<UploadToStageResponse>> {
    let session = ctx.get_session(SessionType::HTTPAPI("UploadToStage".to_string()));
    let context = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    let user_mgr = context.get_user_manager();

    let stage_name = req
        .headers()
        .get("stage_name")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            poem::Error::from_string(
                "Parse stage_name error, not found".to_string(),
                StatusCode::BAD_REQUEST,
            )
        })?;
    let stage = user_mgr
        .get_stage(context.get_tenant().as_str(), stage_name)
        .await
        .map_err(InternalServerError)?;

    let op = StageSource::get_op(&context, &stage)
        .await
        .map_err(InternalServerError)?;

    let relative_path = req
        .headers()
        .get("relative_path")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim_matches('/')
        .to_string();

    let prefix = stage.get_prefix();

    let tenant = context.get_tenant();
    let user = context
        .get_current_user()
        .map_err(InternalServerError)?
        .identity();

    let mut files = vec![];
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = match field.file_name() {
            Some(name) => name.to_string(),
            None => uuid::Uuid::new_v4().to_string(),
        };
        let bytes = field.bytes().await.map_err(InternalServerError)?;
        let file_path = format!("{relative_path}/{name}")
            .trim_start_matches('/')
            .to_string();
        let obj = format!("{prefix}{file_path}");
        let _ = op
            .object(&obj)
            .write(bytes.clone())
            .await
            .map_err(InternalServerError)?;

        if stage.stage_type == StageType::Internal {
            let meta = op
                .object(&obj)
                .metadata()
                .await
                .map_err(InternalServerError)?;
            let file = StageFile {
                path: file_path,
                size: meta.content_length(),
                md5: meta.content_md5(),
                last_modified: meta.last_modified().map_or(DateTime::default(), |t| {
                    Utc.timestamp(t.unix_timestamp(), 0)
                }),
                creator: Some(user.clone()),
            };
            user_mgr
                .add_file(&tenant, &stage.stage_name, file)
                .await
                .map_err(InternalServerError)?;
        }
        files.push(name.clone());
    }

    let mut id = uuid::Uuid::new_v4().to_string();
    Ok(Json(UploadToStageResponse {
        id,
        stage_name: stage_name.to_string(),
        state: "SUCCESS".to_string(),
        files,
    }))
}
