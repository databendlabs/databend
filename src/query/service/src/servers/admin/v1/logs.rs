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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use poem::http::StatusCode;
use poem::Body;
use poem::IntoResponse;

use crate::sessions::SessionManager;

// read log files from cfg.log.log_dir
#[poem::handler]
#[async_backtrace::framed]
pub async fn logs_handler() -> poem::Result<impl IntoResponse> {
    let sessions = SessionManager::instance();
    let data = select_table(&sessions).await.map_err(|err| {
        poem::Error::from_string(
            format!("Failed to fetch log. Error: {err}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;
    Ok(data)
}

async fn select_table(_: &Arc<SessionManager>) -> Result<Body> {
    Err(ErrorCode::Outdated(
        "Outdated. please let us know if you have used it. Will be removed after 2024-05-01",
    ))
}
