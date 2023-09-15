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

/// This file aims to transfer the internal error to the external error, make the error more readable.
/// This will used in the HTTP handler.
use std::collections::HashMap;

use lazy_static::lazy_static;

use crate::ErrorCode;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Error {
    pub name: String,
    pub code: u16,
    pub message: String,
}

lazy_static! {
    // The readable message for the error code.
    static ref ERROR_MESSAGE: HashMap<u16, &'static str> = {
        let mut m = HashMap::new();
        // Parser COPY file content error.
        m.insert(ErrorCode::BadBytes("").code(), "Content included bad bytes");
        m
    };
}

pub struct Errors;

impl Errors {
    // Transfer the internal error(ErrorCode) to the external error message(Error) with json string.
    pub fn get(code: ErrorCode) -> String {
        let message = ERROR_MESSAGE.get(&code.code()).unwrap_or(&"");
        let message = if !message.is_empty() {
            format!("{}: {}", message, code.message())
        } else {
            code.message()
        };

        let error = Error {
            name: code.name(),
            code: code.code(),
            message,
        };
        serde_json::to_string(&error).unwrap_or_else(|_| format!("{:?}", error))
    }
}
