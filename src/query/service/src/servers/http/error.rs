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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_exception::ErrorCode;
use http::StatusCode;
use jwt_simple::prelude::Deserialize;
use poem::error::ResponseError;
use poem::web::Json;
use poem::IntoResponse;
use poem::Response;
use serde::de::StdError;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryError {
    pub code: u16,
    pub message: String,
    pub detail: String,
}

impl QueryError {
    pub(crate) fn from_error_code(e: ErrorCode) -> Self {
        QueryError {
            code: e.code(),
            message: e.display_text(),
            detail: e.detail(),
        }
    }
}

#[derive(Serialize)]
pub struct JsonErrorOnly {
    pub(crate) error: QueryError,
}

// todo(youngsofun): use this in more place
/// turn internal ErrorCode to Http Response with Json Body and proper StatusCode
pub struct JsonErrorCode(pub ErrorCode);

impl ResponseError for JsonErrorCode {
    fn status(&self) -> StatusCode {
        match self.0.code() {
            ErrorCode::AUTHENTICATE_FAILURE
            | ErrorCode::SESSION_TOKEN_EXPIRED
            | ErrorCode::SESSION_TOKEN_NOT_FOUND
            | ErrorCode::REFRESH_TOKEN_EXPIRED
            | ErrorCode::REFRESH_TOKEN_NOT_FOUND
            | ErrorCode::UNKNOWN_USER => StatusCode::UNAUTHORIZED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn as_response(&self) -> Response
    where Self: StdError + Send + Sync + 'static {
        (
            self.status(),
            Json(JsonErrorOnly {
                error: QueryError::from_error_code(self.0.clone()),
            }),
        )
            .into_response()
    }
}

impl Debug for JsonErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for JsonErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for JsonErrorCode {}
