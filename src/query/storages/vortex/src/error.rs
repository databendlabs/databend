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

use databend_common_exception::ErrorCode;
use vortex_error::VortexError;

#[derive(Debug, thiserror::Error)]
pub enum VortexStorageError {
    #[error("Arrow IPC error: {0}")]
    ArrowIpc(String),

    #[error("Vortex error: {0}")]
    Vortex(#[from] VortexError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("{0}")]
    Other(String),
}

impl From<arrow_ipc::reader::ArrowError> for VortexStorageError {
    fn from(e: arrow_ipc::reader::ArrowError) -> Self {
        VortexStorageError::ArrowIpc(e.to_string())
    }
}

impl From<arrow_ipc::writer::ArrowError> for VortexStorageError {
    fn from(e: arrow_ipc::writer::ArrowError) -> Self {
        VortexStorageError::ArrowIpc(e.to_string())
    }
}

impl From<VortexStorageError> for ErrorCode {
    fn from(e: VortexStorageError) -> Self {
        ErrorCode::StorageOther(e.to_string())
    }
}

pub type VortexResult<T> = std::result::Result<T, VortexStorageError>;

/// Convert a VortexResult into a Databend Result.
pub fn into_databend_result<T>(r: VortexResult<T>) -> databend_common_exception::Result<T> {
    r.map_err(ErrorCode::from)
}
