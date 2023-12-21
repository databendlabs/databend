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

use databend_common_grpc::GrpcConnectionError;
use databend_common_meta_types::ConnectionError;
use databend_common_meta_types::MetaNetworkError;

pub(crate) fn grpc_error_to_network_err(e: GrpcConnectionError) -> MetaNetworkError {
    match e {
        GrpcConnectionError::InvalidUri { uri, source } => {
            MetaNetworkError::BadAddressFormat(source.add_context(|| format!("uri: {}", uri)))
        }
        GrpcConnectionError::TLSConfigError { action, source } => {
            MetaNetworkError::TLSConfigError(source.add_context(|| format!("action: {}", action)))
        }
        GrpcConnectionError::CannotConnect { uri, source } => {
            MetaNetworkError::ConnectionError(ConnectionError::new(source, uri))
        }
    }
}
