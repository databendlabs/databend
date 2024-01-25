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

use databend_common_meta_app::share::TableInfoMap;
use models::Credentials;
use models::RequestFile;
use poem::error::BadRequest;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;

use crate::accessor::SharingAccessor;
use crate::models;
use crate::models::PresignFileResponse;
use crate::models::ShareSpec;

#[poem::handler]
#[async_backtrace::framed]
pub async fn share_table_presign_files(
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
    match SharingAccessor::get_share_table_spec_presigned_files(&input).await {
        Ok(output) => Ok(Json(output)),
        Err(e) => Err(BadRequest(e)),
    }
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn share_table_meta(
    credentials: &Credentials,
    Path((_tenant_id, share_name)): Path<(String, String)>,
    Json(request_tables): Json<Vec<String>>,
) -> PoemResult<Json<TableInfoMap>> {
    let requester = credentials.token.clone();
    let input = models::TableMetaLambdaInput::new(
        credentials.token.clone(),
        share_name,
        requester,
        request_tables,
        None,
    );
    match SharingAccessor::get_share_table_meta(&input).await {
        Ok(output) => Ok(Json(output)),
        Err(e) => Err(BadRequest(e)),
    }
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn share_spec(
    _credentials: &Credentials,
    Path(tenant_id): Path<String>,
    Json(_request_tables): Json<Vec<String>>,
) -> PoemResult<Json<Vec<ShareSpec>>> {
    match SharingAccessor::get_share_spec(&tenant_id).await {
        Ok(output) => Ok(Json(output)),
        Err(e) => Err(BadRequest(e)),
    }
}
