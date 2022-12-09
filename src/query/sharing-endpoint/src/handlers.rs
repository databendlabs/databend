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

use models::Credentials;
use models::RequestFile;
use poem::error::BadRequest;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;

use crate::accessor::SharingAccessor;
use crate::models;
use crate::models::PresignFileResponse;

#[poem::handler]
pub async fn presign_files(
    credentials: &Credentials,
    Path((_tenant_id, share_name, table_name)): Path<(String, String, String)>,
    Json(request_files): Json<Vec<RequestFile>>,
) -> PoemResult<Json<Vec<PresignFileResponse>>> {
    let requester = credentials.token.clone();
    let input = models::LambdaInput::new(
        credentials.token.clone(),
        share_name,
        requester,
        table_name,
        request_files,
        None,
    );
    return match SharingAccessor::get_presigned_files(&input).await {
        Ok(output) => Ok(Json(output)),
        Err(e) => Err(BadRequest(e)),
    };
}
