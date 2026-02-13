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

use std::error::Error;

use databend_common_exception::ErrorCode;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_common_meta_app::tenant_key::resource::TenantResource;
use databend_meta_types::MetaError;

/// CRUD Error that can be an API level error or a business error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum CrudError<E>
where E: Error + 'static
{
    #[error(transparent)]
    ApiError(#[from] MetaError),

    #[error(transparent)]
    Business(E),
}

impl<E> CrudError<E>
where E: Error + 'static
{
    pub fn is_business_error(&self) -> bool {
        match self {
            CrudError::ApiError(_e) => false,
            CrudError::Business(_e) => true,
        }
    }

    /// Convert the error into a layered result.
    pub fn into_result(self) -> Result<Result<(), E>, MetaError> {
        match self {
            CrudError::ApiError(meta_err) => Err(meta_err),
            CrudError::Business(e) => Ok(Err(e)),
        }
    }
}

impl<R> From<ExistError<R>> for CrudError<ExistError<R>>
where R: TenantResource + 'static
{
    fn from(e: ExistError<R>) -> Self {
        CrudError::Business(e)
    }
}

impl<R> From<UnknownError<R>> for CrudError<UnknownError<R>>
where R: TenantResource + 'static
{
    fn from(e: UnknownError<R>) -> Self {
        CrudError::Business(e)
    }
}

impl<E> From<CrudError<E>> for ErrorCode
where
    E: Into<ErrorCode>,
    E: Error,
{
    fn from(value: CrudError<E>) -> Self {
        match value {
            CrudError::ApiError(meta_err) => ErrorCode::MetaServiceError(meta_err.to_string()),
            CrudError::Business(e) => e.into(),
        }
    }
}
