// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use mysql::Error as MysqlClientError;
use reqwest::Error as HttpClientError;
use serde_json::Error as SerdeJsonError;
use sqllogictest::TestError;
use thiserror::Error;
use walkdir::Error as WalkDirError;

pub type Result<T> = std::result::Result<T, DSqlLogicTestError>;

#[derive(Debug, Error)]
pub enum DSqlLogicTestError {
    // Error from sqllogictest-rs
    #[error("SqlLogicTest error(from sqllogictest-rs crate): {0}")]
    SqlLogicTest(TestError),
    // Error from databend
    #[error("Databend error: {0}")]
    Databend(ErrorCode),
    // Error from mysql client
    #[error("Mysql client error: {0}")]
    MysqlClient(MysqlClientError),
    // Error from http client
    #[error("Http client error(from reqwest crate): {0}")]
    HttpClient(HttpClientError),
    // Error from WalkDir
    #[error("Walk dir error: {0}")]
    WalkDir(WalkDirError),
    // Error from serde json
    #[error("Serde json error: {0}")]
    SerdeJson(SerdeJsonError),
    // Error from databend sqllogictests
    #[error("Databend sqllogictests error: {0}")]
    SelfError(String),
}

impl From<TestError> for DSqlLogicTestError {
    fn from(value: TestError) -> Self {
        DSqlLogicTestError::SqlLogicTest(value)
    }
}

impl From<ErrorCode> for DSqlLogicTestError {
    fn from(value: ErrorCode) -> Self {
        DSqlLogicTestError::Databend(value)
    }
}

impl From<MysqlClientError> for DSqlLogicTestError {
    fn from(value: MysqlClientError) -> Self {
        DSqlLogicTestError::MysqlClient(value)
    }
}

impl From<HttpClientError> for DSqlLogicTestError {
    fn from(value: HttpClientError) -> Self {
        DSqlLogicTestError::HttpClient(value)
    }
}

impl From<WalkDirError> for DSqlLogicTestError {
    fn from(value: WalkDirError) -> Self {
        DSqlLogicTestError::WalkDir(value)
    }
}

impl From<SerdeJsonError> for DSqlLogicTestError {
    fn from(value: SerdeJsonError) -> Self {
        DSqlLogicTestError::SerdeJson(value)
    }
}

impl From<String> for DSqlLogicTestError {
    fn from(value: String) -> Self {
        DSqlLogicTestError::SelfError(value)
    }
}
