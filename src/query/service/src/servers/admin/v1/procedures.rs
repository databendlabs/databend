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

use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_system::ProceduresTable;
use http::StatusCode;
use poem::IntoResponse;
use poem::web::Json;
use poem::web::Path;
use poem::web::Query;

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_procedures(Path(tenant): Path<String>) -> poem::Result<impl IntoResponse> {
    match ProceduresTable::get_procedures(&Tenant::new_literal(&tenant)).await {
        Ok(procedures) => Ok(Json(procedures)),
        Err(cause) => Err(poem::Error::from_string(
            format!("failed to list procedures. cause: {:?}", cause),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn get_procedure_by_id(
    Path((tenant, procedure_id)): Path<(String, u64)>,
) -> poem::Result<impl IntoResponse> {
    match ProceduresTable::get_procedure_by_id(&Tenant::new_literal(&tenant), procedure_id).await {
        Ok(Some(procedure)) => Ok(Json(procedure)),
        Ok(None) => Err(poem::Error::from_string(
            format!("procedure not found with id: {}", procedure_id),
            StatusCode::NOT_FOUND,
        )),
        Err(cause) => Err(poem::Error::from_string(
            format!("failed to get procedure. cause: {:?}", cause),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn get_procedure_by_name(
    Path((tenant, name)): Path<(String, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> poem::Result<impl IntoResponse> {
    let args = params.get("args").map(|s| s.as_str()).unwrap_or("");
    match ProceduresTable::get_procedure(&Tenant::new_literal(&tenant), &name, args).await {
        Ok(Some(procedure)) => Ok(Json(procedure)),
        Ok(None) => Err(poem::Error::from_string(
            format!("procedure not found: {}({})", name, args),
            StatusCode::NOT_FOUND,
        )),
        Err(cause) => Err(poem::Error::from_string(
            format!("failed to get procedure. cause: {:?}", cause),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}
