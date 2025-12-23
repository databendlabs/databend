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
use poem::IntoResponse;
use poem::Response;
use poem::error::ResponseError;
use poem::web::Json;
use serde::Serialize;
use serde::de::StdError;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryError {
    pub code: u16,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl QueryError {
    pub(crate) fn from_error_code<C>(e: ErrorCode<C>) -> Self {
        // TODO(andylokandy): display error stack
        QueryError {
            code: e.code(),
            message: e.display_text(),
            detail: if e.detail().is_empty() {
                None
            } else {
                Some(e.detail().clone())
            },
        }
    }
}

#[derive(Serialize)]
pub struct JsonErrorOnly {
    pub(crate) error: QueryError,
}

// todo(youngsofun): use this in more place
/// turn internal ErrorCode to Http Response with Json Body and proper StatusCode
#[derive(Debug)]
pub struct HttpErrorCode {
    error_code: ErrorCode,
    status_code: Option<StatusCode>,
}

impl HttpErrorCode {
    pub fn new(error_code: ErrorCode, status_code: StatusCode) -> Self {
        Self {
            error_code,
            status_code: Some(status_code),
        }
    }

    pub fn error_code(error_code: ErrorCode) -> Self {
        Self {
            error_code,
            status_code: None,
        }
    }

    pub fn bad_request(error_code: ErrorCode) -> Self {
        Self::new(error_code, StatusCode::BAD_REQUEST)
    }

    pub fn server_error(error_code: ErrorCode) -> Self {
        Self::new(error_code, StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl ResponseError for HttpErrorCode {
    fn status(&self) -> StatusCode {
        if let Some(s) = self.status_code {
            return s;
        }
        match self.error_code.code() {
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
                error: QueryError::from_error_code(self.error_code.clone()),
            }),
        )
            .into_response()
    }
}

impl Display for HttpErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} {}", self.status_code, self.error_code)
    }
}

impl std::error::Error for HttpErrorCode {}
