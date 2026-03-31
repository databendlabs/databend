// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::Debug;
use std::io;

use anyhow::anyhow;
use iceberg::{Error, ErrorKind};
use volo_thrift::MaybeException;

/// Format a thrift error into iceberg error.
///
/// Please only throw this error when you are sure that the error is caused by thrift.
pub fn from_thrift_error(error: impl std::error::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Operation failed for hitting thrift error".to_string(),
    )
    .with_source(anyhow!("thrift error: {error:?}"))
}

/// Format a thrift exception into iceberg error.
pub fn from_thrift_exception<T, E: Debug>(value: MaybeException<T, E>) -> Result<T, Error> {
    match value {
        MaybeException::Ok(v) => Ok(v),
        MaybeException::Exception(err) => Err(Error::new(
            ErrorKind::Unexpected,
            "Operation failed for hitting thrift error".to_string(),
        )
        .with_source(anyhow!("thrift error: {err:?}"))),
    }
}

/// Format an io error into iceberg error.
pub fn from_io_error(error: io::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Operation failed for hitting io error".to_string(),
    )
    .with_source(error)
}
