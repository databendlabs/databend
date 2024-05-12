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

use databend_common_base::mem_allocator::dump_profile;
use databend_common_exception::ErrorCode;
use http::StatusCode;
use poem::error::InternalServerError;
use poem::web::IntoResponse;
use poem::web::Query;
use poem::Error;
use tempfile::Builder;

#[poem::handler]
pub async fn debug_jeprof_dump_handler(
    _req: Option<Query<String>>,
) -> poem::Result<impl IntoResponse> {
    let tmp_file = Builder::new()
        .prefix("heap_dump_")
        .suffix(".prof")
        .tempfile()
        .map_err(InternalServerError)?;

    let path = tmp_file.path();
    let body = dump_profile(&path.to_string_lossy()).map_err(|e| {
        Error::new(
            ErrorCode::from(e.to_string().as_str()),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;
    let body_len = body.len();

    Ok(body.with_header("content-length", body_len).with_header(
        "Content-Disposition",
        format!("attachment; filename=\"{}\"", &path.to_string_lossy()),
    ))
}
