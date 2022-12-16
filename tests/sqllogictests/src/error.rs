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

use std::error;
use std::fmt::Display;
use std::fmt::Formatter;

use common_exception::ErrorCode;
use mysql::Error as MysqlClientError;
use reqwest::Error as HttpClientError;
use serde_json::Error as SerdeJsonError;
use sqllogictest::TestError;
use walkdir::Error as WalkDirError;

pub type Result<T> = std::result::Result<T, DSqlLogicTestError>;

#[derive(Debug)]
pub enum DSqlLogicTestError {
    // Error from sqllogictest-rs
    SqlLogicTest(TestError),
    // Error from databend
    Databend(ErrorCode),
    // Error from mysql client
    MysqlClient(MysqlClientError),
    // Error from http client
    HttpClient(HttpClientError),
    // Error from WalkDir
    WalkDir(WalkDirError),
    // Error from serde json
    SerdeJson(SerdeJsonError),
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

impl Display for DSqlLogicTestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DSqlLogicTestError::SqlLogicTest(error) => write!(
                f,
                "SqlLogicTest error(from sqllogictest-rs) crate: {}",
                error
            ),
            DSqlLogicTestError::Databend(error) => write!(f, "Databend error: {}", error),
            DSqlLogicTestError::MysqlClient(error) => write!(f, "Mysql client error: {}", error),
            DSqlLogicTestError::HttpClient(error) => write!(f, "Http client error: {}", error),
            DSqlLogicTestError::WalkDir(error) => write!(f, "Walk dir error: {}", error),
            DSqlLogicTestError::SerdeJson(error) => write!(f, "Serde json error: {}", error),
        }
    }
}

impl error::Error for DSqlLogicTestError {}
