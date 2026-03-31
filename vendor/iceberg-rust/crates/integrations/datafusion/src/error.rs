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

use anyhow::anyhow;
use iceberg::{Error, ErrorKind};

/// Converts a datafusion error into an iceberg error.
pub fn from_datafusion_error(error: datafusion::error::DataFusionError) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Operation failed for hitting datafusion error".to_string(),
    )
    .with_source(anyhow!("datafusion error: {error:?}"))
}
/// Converts an iceberg error into a datafusion error.
pub fn to_datafusion_error(error: Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(error.into())
}
