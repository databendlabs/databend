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

use databend_common_meta_app::UnknownOrExistsError;

/// Error returned by one transaction-building attempt.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum KvApiOrUserError<KVError, UserError> {
    #[error("fail to access meta-store: {0}")]
    KvApi(#[from] KVError),

    #[error(transparent)]
    User(UserError),
}

impl<KVError, UserError> KvApiOrUserError<KVError, UserError> {
    pub fn user<E>(error: E) -> Self
    where UserError: From<E> {
        Self::User(UserError::from(error))
    }
}

impl<KVError, UnknownError, ExistError>
    KvApiOrUserError<KVError, UnknownOrExistsError<UnknownError, ExistError>>
{
    pub fn unknown(error: UnknownError) -> Self {
        Self::User(UnknownOrExistsError::Unknown(error))
    }

    pub fn exists(error: ExistError) -> Self {
        Self::User(UnknownOrExistsError::Exists(error))
    }
}
