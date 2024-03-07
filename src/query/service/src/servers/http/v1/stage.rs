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

use databend_common_meta_app::principal::StageInfo;
use databend_common_storages_stage::StageTable;
use databend_common_users::UserApiProvider;
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
use crate::sessions::TableContext;

#[derive(Serialize, Deserialize, Debug)]
pub struct UploadToStageResponse {
    pub id: String,
    pub stage_name: String,
    pub state: String,
    pub files: Vec<String>,
}

#[derive(Debug)]
struct UploadToStageArgs {
    stage_name: String,
    relative_path: String,
}

impl UploadToStageArgs {
    pub fn parse(req: &Request) -> PoemResult<UploadToStageArgs> {
        let stage_name = Self::read_arg(req, "stage-name")
            .ok_or_else(|| {
                poem::Error::from_string(
                    "Parse stage_name error, please check your arguments".to_string(),
                    StatusCode::BAD_REQUEST,
                )
            })?
            .to_string();

        let relative_path = Self::read_arg(req, "relative-path")
            .unwrap_or("")
            .trim_matches('/')
            .to_string();

        Ok(UploadToStageArgs {
            stage_name,
            relative_path,
        })
    }

    // read_arg parses the http request to retrieve the arguments. In the before, we use
    // argument like `stage_name` in the header, but having underscore in the header is
    // not a good practice. So we change the argument to `x-databend-stage-name` in the
    // header, but we still need to support the old argument for backward compatibility.
    fn read_arg<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
        let mut arg = req.headers().get(format!("x-databend-{}", name).as_str());
        // if "x-databend-stage-name" is not found, try "stage_name", which is for backward compatibility
        if arg.is_none() {
            arg = req.headers().get(name.replace('-', "_").as_str());
        }
        arg.and_then(|v| v.to_str().ok())
    }
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn upload_to_stage(
    ctx: &HttpQueryContext,
    req: &Request,
    mut multipart: Multipart,
) -> PoemResult<Json<UploadToStageResponse>> {
    let session = ctx.upgrade_session(SessionType::HTTPAPI("UploadToStage".to_string()))?;
    let context = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;
    let args = UploadToStageArgs::parse(req)?;

    let stage = if args.stage_name == "~" {
        StageInfo::new_user_stage(
            context
                .get_current_user()
                .map_err(InternalServerError)?
                .name
                .as_str(),
        )
    } else {
        UserApiProvider::instance()
            .get_stage(&context.get_tenant(), &args.stage_name)
            .await
            .map_err(InternalServerError)?
    };

    let op = StageTable::get_op(&stage).map_err(InternalServerError)?;

    let mut files = vec![];
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = match field.file_name() {
            Some(name) => name.to_string(),
            None => uuid::Uuid::new_v4().to_string(),
        };
        let bytes = field.bytes().await.map_err(InternalServerError)?;
        let file_path = format!("{}/{}", args.relative_path, name)
            .trim_start_matches('/')
            .to_string();
        let _ = op
            .write(&file_path, bytes)
            .await
            .map_err(InternalServerError)?;

        files.push(name.clone());
    }

    let mut id = uuid::Uuid::new_v4().to_string();
    Ok(Json(UploadToStageResponse {
        id,
        stage_name: args.stage_name,
        state: "SUCCESS".to_string(),
        files,
    }))
}
