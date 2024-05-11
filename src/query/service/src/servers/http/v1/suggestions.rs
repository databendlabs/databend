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

use databend_enterprise_background_service::Suggestion;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::http::StatusCode;
use poem::web::Json;
use poem::Request;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;
use crate::sessions::SessionType;
use crate::table_functions::SuggestedBackgroundTasksSource;

#[derive(Serialize, Deserialize, Debug)]
pub struct SuggestionsResponse {
    pub suggestions: Vec<Suggestion>,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_suggestions(
    ctx: &HttpQueryContext,
    _req: &Request,
) -> PoemResult<Json<SuggestionsResponse>> {
    let session = ctx.upgrade_session(SessionType::HTTPAPI("ListSuggestions".to_string()))?;
    let context = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;
    let suggestions = SuggestedBackgroundTasksSource::all_suggestions(context)
        .await
        .map_err(|err| poem::Error::from_string(err.message(), StatusCode::BAD_REQUEST))?;
    Ok(Json(SuggestionsResponse { suggestions }))
}
