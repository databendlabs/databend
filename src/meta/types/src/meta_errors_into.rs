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

use crate::MetaError;
use crate::MetaNetworkError;

impl From<MetaError> for ErrorCode {
    fn from(e: MetaError) -> Self {
        match e {
            MetaError::NetworkError(net_err) => net_err.into(),
            MetaError::StorageError(sto_err) => sto_err.into(),
            MetaError::ClientError(ce) => ce.into(),
            MetaError::APIError(e) => e.into(),
        }
    }
}

impl From<tonic::Status> for MetaError {
    fn from(status: tonic::Status) -> Self {
        let net_err = MetaNetworkError::from(status);
        MetaError::NetworkError(net_err)
    }
}
