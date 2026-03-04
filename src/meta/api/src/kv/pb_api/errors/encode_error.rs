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

use databend_common_proto_conv::Incompatible;
use databend_meta_types::InvalidArgument;
use databend_meta_types::MetaError;
use databend_meta_types::MetaNetworkError;

/// An error occurred when encoding with FromToProto.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("PbEncodeError: {0}")]
pub enum PbEncodeError {
    EncodeError(#[from] prost::EncodeError),
    Incompatible(#[from] Incompatible),
}

impl From<PbEncodeError> for MetaError {
    fn from(value: PbEncodeError) -> Self {
        match value {
            PbEncodeError::EncodeError(e) => MetaError::from(InvalidArgument::new(e, "")),
            PbEncodeError::Incompatible(e) => MetaError::from(InvalidArgument::new(e, "")),
        }
    }
}

impl From<PbEncodeError> for MetaNetworkError {
    fn from(value: PbEncodeError) -> Self {
        match value {
            PbEncodeError::EncodeError(e) => MetaNetworkError::from(InvalidArgument::new(e, "")),
            PbEncodeError::Incompatible(e) => MetaNetworkError::from(InvalidArgument::new(e, "")),
        }
    }
}
