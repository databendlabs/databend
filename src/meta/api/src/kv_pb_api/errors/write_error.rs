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

use databend_meta_types::MetaError;

use crate::kv_pb_api::errors::PbDecodeError;
use crate::kv_pb_api::errors::PbEncodeError;

/// An error occurs when writing protobuf encoded value to kv store.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("PbApiWriteError: {0}")]
pub enum PbApiWriteError<E> {
    PbEncodeError(#[from] PbEncodeError),
    /// upsert reads the state transition after the operation.
    PbDecodeError(#[from] PbDecodeError),
    /// Error returned from KVApi.
    KvApiError(E),
}

impl From<PbApiWriteError<MetaError>> for MetaError {
    /// For KVApi that returns MetaError, convert protobuf related error to MetaError directly.
    fn from(value: PbApiWriteError<MetaError>) -> Self {
        match value {
            PbApiWriteError::PbEncodeError(e) => MetaError::from(e),
            PbApiWriteError::PbDecodeError(e) => MetaError::from(e),
            PbApiWriteError::KvApiError(e) => e,
        }
    }
}
