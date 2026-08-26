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

use databend_common_meta_app::app_error::TxnRetryMaxTimes;

use super::KvApiOrUserError;

/// Error returned when driving a [`MetaTxn`](super::MetaTxn) retry loop.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum RunError<KVError, UserError> {
    #[error("fail to access meta-store: {0}")]
    KvApi(KVError),

    #[error(transparent)]
    TxnRetryMaxTimes(TxnRetryMaxTimes),

    #[error(transparent)]
    User(UserError),
}

impl<KVError, UserError> From<KvApiOrUserError<KVError, UserError>>
    for RunError<KVError, UserError>
{
    fn from(error: KvApiOrUserError<KVError, UserError>) -> Self {
        match error {
            KvApiOrUserError::KvApi(error) => Self::KvApi(error),
            KvApiOrUserError::User(error) => Self::User(error),
        }
    }
}

impl<KVError, UserError> From<TxnRetryMaxTimes> for RunError<KVError, UserError> {
    fn from(error: TxnRetryMaxTimes) -> Self {
        Self::TxnRetryMaxTimes(error)
    }
}
