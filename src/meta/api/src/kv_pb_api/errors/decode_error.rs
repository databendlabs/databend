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

use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaError;
use databend_common_proto_conv::Incompatible;

/// An error occurred when decoding protobuf encoded value.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("PbDecodeError: {0}")]
pub enum PbDecodeError {
    DecodeError(#[from] prost::DecodeError),
    Incompatible(#[from] Incompatible),
}

impl From<PbDecodeError> for MetaError {
    fn from(value: PbDecodeError) -> Self {
        match value {
            PbDecodeError::DecodeError(e) => MetaError::from(InvalidReply::new("", &e)),
            PbDecodeError::Incompatible(e) => MetaError::from(InvalidReply::new("", &e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::kv_pb_api::errors::PbDecodeError;

    #[test]
    fn test_error_message() {
        let e = PbDecodeError::DecodeError(prost::DecodeError::new("decode error"));
        assert_eq!(
            "PbDecodeError: failed to decode Protobuf message: decode error",
            e.to_string()
        );
    }
}
