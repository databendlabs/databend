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

use anyerror::AnyError;
use databend_common_meta_runtime_api::ChannelError;
use databend_common_meta_types::ConnectionError;
use databend_common_meta_types::MetaNetworkError;

pub(crate) fn channel_error_to_network_err(e: ChannelError) -> MetaNetworkError {
    match e {
        ChannelError::InvalidUri { ref uri, .. } => {
            MetaNetworkError::BadAddressFormat(AnyError::new(&e).add_context(|| uri.clone()))
        }
        ChannelError::TlsConfig { ref action, .. } => {
            MetaNetworkError::TLSConfigError(AnyError::new(&e).add_context(|| action.clone()))
        }
        ChannelError::CannotConnect { ref uri, .. } => {
            MetaNetworkError::ConnectionError(ConnectionError::new(AnyError::new(&e), uri.clone()))
        }
    }
}
