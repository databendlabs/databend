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

#[derive(Debug, Clone, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    BadAddressFormat(AnyError),

    #[error("can not listen {listening}: {message}")]
    ListenError { message: String, listening: String },

    #[error(transparent)]
    TlsConfigError(AnyError),
}

impl HttpError {
    pub fn listen_error(listening: impl ToString, message: impl ToString) -> Self {
        Self::ListenError {
            message: message.to_string(),
            listening: listening.to_string(),
        }
    }
}
