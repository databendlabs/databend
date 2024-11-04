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
use databend_common_storages_system::UserFunctionsTable;
use http::StatusCode;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;

#[poem::handler]
#[async_backtrace::framed]
pub async fn user_functions(Path(tenant): Path<String>) -> poem::Result<impl IntoResponse> {
    match UserFunctionsTable::get_udfs(&Tenant::new_literal(&tenant)).await {
        Ok(v) => Ok(Json(v)),
        Err(cause) => Err(poem::Error::from_string(
            format!("failed to user functions. cause: {:?}", cause),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}
