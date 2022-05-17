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

use common_io::prelude::get_abs_path;
use common_io::prelude::StorageParams;
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

    let op = context
        .get_storage_operator()
        .map_err(InternalServerError)?;
    let mut files = vec![];

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

    let relative_path = req
        .headers()
        .get("relative_path")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim_matches(|c| c == '/');

    let final_related_path: String;
    match stage.stage_type {
        // It's internal, so we already have an op which has the root path
        // need to inject a tenant path
        StageType::Internal => {
            let prefix = format!("stage/{}", stage.stage_name);
            final_related_path = get_abs_path(prefix.as_str(), relative_path);
        }
        // It's  external, so we need to join the root path
        StageType::External => match stage.stage_params.storage {
            StorageParams::S3(ref s3) => {
                final_related_path = get_abs_path(s3.root.as_str(), relative_path);
            }
            _ => todo!("other stage is not supported"),
        },
    }

    while let Ok(Some(field)) = multipart.next_field().await {
        let name = match field.file_name() {
            Some(name) => name.to_string(),
            None => uuid::Uuid::new_v4().to_string(),
        };
        let bytes = field.bytes().await.map_err(InternalServerError)?;
        let obj = format!("{}/{}", final_related_path, name);
        let _ = op
            .object(&obj)
            .write(bytes)
            .await
            .map_err(InternalServerError)?;
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
