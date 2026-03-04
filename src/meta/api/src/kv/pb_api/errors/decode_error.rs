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

use std::fmt;

use databend_common_proto_conv::Incompatible;
use databend_meta_types::InvalidReply;
use databend_meta_types::MetaError;

/// An error occurred when decoding protobuf encoded value.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub struct PbDecodeError {
    pub error: ErrorEnum,
    pub context: String,
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("{0}")]
pub enum ErrorEnum {
    DecodeError(#[from] prost::DecodeError),
    Incompatible(#[from] Incompatible),
}

impl fmt::Display for PbDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PbDecodeError: {}", self.error)?;
        if !self.context.is_empty() {
            write!(f, "; when:({})", self.context)?;
        }
        Ok(())
    }
}

impl From<prost::DecodeError> for PbDecodeError {
    fn from(value: prost::DecodeError) -> Self {
        PbDecodeError {
            error: ErrorEnum::DecodeError(value),
            context: "".to_string(),
        }
    }
}

impl From<Incompatible> for PbDecodeError {
    fn from(value: Incompatible) -> Self {
        PbDecodeError {
            error: ErrorEnum::Incompatible(value),
            context: "".to_string(),
        }
    }
}

impl From<PbDecodeError> for MetaError {
    fn from(value: PbDecodeError) -> Self {
        MetaError::from(InvalidReply::new("", &value))
    }
}

impl PbDecodeError {
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = context.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::kv_pb_api::errors::PbDecodeError;

    #[test]
    fn test_error_message() {
        let e = PbDecodeError::from(prost::DecodeError::new("decode error"));
        assert_eq!(
            "PbDecodeError: failed to decode Protobuf message: decode error",
            e.to_string()
        );

        let e = PbDecodeError::from(prost::DecodeError::new("decode error")).with_context("ctx");
        assert_eq!(
            "PbDecodeError: failed to decode Protobuf message: decode error; when:(ctx)",
            e.to_string()
        );
    }
}
