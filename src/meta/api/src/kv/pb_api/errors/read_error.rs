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

use databend_meta_kvapi::kvapi;
use databend_meta_types::InvalidReply;
use databend_meta_types::MetaError;
use databend_meta_types::anyerror::AnyError;

use crate::kv_pb_api::errors::PbDecodeError;

/// An error occurs when found an unexpected None value.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("NoneValue: unexpected None value of key: '{key}'")]
pub struct NoneValue {
    key: String,
}

impl NoneValue {
    pub fn new(key: impl ToString) -> Self {
        NoneValue {
            key: key.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("StreamReadEOF: expected {expected} items but only received {received} items")]
pub struct StreamReadEof {
    expected: u64,
    received: u64,
}

impl StreamReadEof {
    pub fn new(expected: u64, received: u64) -> Self {
        StreamReadEof { expected, received }
    }

    pub fn set_received(&mut self, received: u64) {
        self.received = received;
    }
}

/// An error occurs when reading protobuf encoded value from kv store.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("PbApiReadError: {0}")]
pub enum PbApiReadError<E> {
    PbDecodeError(#[from] PbDecodeError),
    KeyError(#[from] kvapi::KeyError),
    NoneValue(#[from] NoneValue),
    StreamReadEof(#[from] StreamReadEof),
    /// Error returned from KVApi.
    KvApiError(E),
}

impl From<PbApiReadError<MetaError>> for MetaError {
    /// For KVApi that returns MetaError, convert protobuf related error to MetaError directly.
    ///
    /// Because MetaError contains network protocol level error variant.
    /// If there is a decoding error, consider it as network level error.
    fn from(value: PbApiReadError<MetaError>) -> Self {
        match value {
            PbApiReadError::PbDecodeError(e) => MetaError::from(e),
            PbApiReadError::KeyError(e) => MetaError::from(InvalidReply::new("", &e)),
            PbApiReadError::NoneValue(e) => MetaError::from(InvalidReply::new("", &e)),
            PbApiReadError::StreamReadEof(e) => {
                MetaError::from(InvalidReply::new(e.to_string(), &AnyError::error("")))
            }
            PbApiReadError::KvApiError(e) => e,
        }
    }
}
