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

use std::any::type_name;
use std::io;

use databend_common_exception::ErrorCode;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::TenantIsEmpty;
use databend_common_meta_app::app_error::TxnRetryMaxTimes;
use databend_meta_types::InvalidArgument;
use databend_meta_types::InvalidReply;
use databend_meta_types::MetaAPIError;
use databend_meta_types::MetaClientError;
use databend_meta_types::MetaError;
use databend_meta_types::MetaNetworkError;
use tonic::Status;

use super::txn_error::MetaTxnError;

/// Errors for a kvapi::KVApi based application, such SchemaApi, ShareApi.
///
/// There are three subset of errors in it:
///
/// - (1) AppError: the errors that relate to the application of meta but not about meta itself.
///
/// - (2) Meta data errors raised by a embedded meta-store(not the remote meta-service): StorageError.
///
/// - (3) Meta data errors returned when accessing the remote meta-store service:
///   - ClientError: errors returned when creating a client.
///   - NetworkError: errors returned when sending/receiving RPC to/from a remote meta-store service.
///   - APIError: errors returned by the remote meta-store service.
///
/// Either a local or remote meta-store will returns (1) AppError.
/// An embedded meta-store only returns (1) and (2), while a remote meta-store service only returns (1) and (3)
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum KVAppError {
    /// An error that indicates something wrong for the application of kvapi::KVApi, but nothing wrong about meta.
    #[error(transparent)]
    AppError(#[from] AppError),

    #[error("fail to access meta-store: {0}")]
    MetaError(#[from] MetaError),
}

impl From<KVAppError> for ErrorCode {
    fn from(e: KVAppError) -> Self {
        match e {
            KVAppError::AppError(app_err) => app_err.into(),
            KVAppError::MetaError(meta_err) => ErrorCode::MetaServiceError(meta_err.to_string()),
        }
    }
}

impl From<MetaTxnError> for KVAppError {
    fn from(value: MetaTxnError) -> Self {
        match value {
            MetaTxnError::TxnRetryMaxTimes(e) => Self::AppError(AppError::from(e)),
            MetaTxnError::MetaError(e) => Self::MetaError(e),
        }
    }
}

impl From<TxnRetryMaxTimes> for KVAppError {
    fn from(value: TxnRetryMaxTimes) -> Self {
        KVAppError::AppError(AppError::from(value))
    }
}

impl From<Status> for KVAppError {
    fn from(s: Status) -> Self {
        let meta_err = MetaError::from(s);
        Self::MetaError(meta_err)
    }
}

impl From<io::Error> for KVAppError {
    fn from(e: io::Error) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<MetaClientError> for KVAppError {
    fn from(e: MetaClientError) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<MetaNetworkError> for KVAppError {
    fn from(e: MetaNetworkError) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<InvalidArgument> for KVAppError {
    fn from(value: InvalidArgument) -> Self {
        let network_error = MetaNetworkError::from(value);
        Self::MetaError(MetaError::from(network_error))
    }
}

impl From<MetaAPIError> for KVAppError {
    fn from(e: MetaAPIError) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<InvalidReply> for KVAppError {
    fn from(e: InvalidReply) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<TenantIsEmpty> for KVAppError {
    fn from(value: TenantIsEmpty) -> Self {
        KVAppError::AppError(AppError::from(value))
    }
}

impl TryInto<MetaAPIError> for KVAppError {
    type Error = InvalidReply;

    fn try_into(self) -> Result<MetaAPIError, Self::Error> {
        match self {
            KVAppError::AppError(app_err) => Err(InvalidReply::new(
                format!(
                    "expect: {}, got: {}",
                    type_name::<MetaAPIError>(),
                    typ(&app_err)
                ),
                &app_err,
            )),
            KVAppError::MetaError(meta_err) => match meta_err {
                MetaError::APIError(api_err) => Ok(api_err),
                e => Err(InvalidReply::new(
                    format!("expect: {}, got: {}", type_name::<MetaAPIError>(), typ(&e)),
                    &e,
                )),
            },
        }
    }
}

impl TryInto<MetaError> for KVAppError {
    type Error = InvalidReply;

    fn try_into(self) -> Result<MetaError, Self::Error> {
        match self {
            KVAppError::AppError(app_err) => Err(InvalidReply::new(
                format!(
                    "expect: {}, got: {}",
                    type_name::<MetaError>(),
                    typ(&app_err)
                ),
                &app_err,
            )),
            KVAppError::MetaError(meta_err) => Ok(meta_err),
        }
    }
}

fn typ<T>(_v: &T) -> &'static str {
    type_name::<T>()
}

impl KVAppError {
    /// Split the error into `AppError` or `MetaError`.
    ///
    /// Returns `Ok(AppError)` for application-level errors,
    /// or `Err(MetaError)` for infrastructure errors.
    pub fn split(self) -> Result<AppError, MetaError> {
        match self {
            KVAppError::AppError(e) => Ok(e),
            KVAppError::MetaError(e) => Err(e),
        }
    }
}

/// Extension trait to convert `Result<T, KVAppError>` into nested result form.
///
/// This allows using the `?` operator to propagate `MetaError` while handling
/// `AppError` explicitly at call sites.
///
/// # Example
///
/// ```ignore
/// use databend_common_meta_api::KVAppResultExt;
///
/// // Before: verbose pattern matching
/// let result = api.create_database(req).await;
/// match result {
///     Ok(reply) => { /* handle success */ }
///     Err(KVAppError::AppError(e)) => { /* handle app error */ }
///     Err(KVAppError::MetaError(e)) => return Err(e.into()),
/// }
///
/// // After: use ? for MetaError, handle AppError directly
/// let result = api.create_database(req).await.into_nested()?;
/// match result {
///     Ok(reply) => { /* handle success */ }
///     Err(app_err) => { /* just AppError */ }
/// }
/// ```
pub trait KVAppResultExt<T> {
    /// Convert `Result<T, KVAppError>` into `Result<Result<T, AppError>, MetaError>`.
    ///
    /// - `Ok(v)` becomes `Ok(Ok(v))`
    /// - `Err(KVAppError::AppError(e))` becomes `Ok(Err(e))`
    /// - `Err(KVAppError::MetaError(e))` becomes `Err(e)`
    fn into_nested(self) -> Result<Result<T, AppError>, MetaError>;
}

impl<T> KVAppResultExt<T> for Result<T, KVAppError> {
    fn into_nested(self) -> Result<Result<T, AppError>, MetaError> {
        match self {
            Ok(v) => Ok(Ok(v)),
            Err(KVAppError::AppError(e)) => Ok(Err(e)),
            Err(KVAppError::MetaError(e)) => Err(e),
        }
    }
}

/// Convert nested result back to flat `Result<T, KVAppError>`.
///
/// This is the inverse of [`KVAppResultExt::into_nested`].
pub fn from_nested<T>(nested: Result<Result<T, AppError>, MetaError>) -> Result<T, KVAppError> {
    match nested {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(app_err)) => Err(KVAppError::AppError(app_err)),
        Err(meta_err) => Err(KVAppError::MetaError(meta_err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_app_error() -> AppError {
        AppError::from(TenantIsEmpty::new("test_context"))
    }

    fn sample_meta_error() -> MetaError {
        MetaError::from(MetaNetworkError::GetNodeAddrError("test_error".to_string()))
    }

    #[test]
    fn test_split_app_error() {
        let kv_err = KVAppError::AppError(sample_app_error());
        let result = kv_err.split();
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AppError::TenantIsEmpty(_)));
    }

    #[test]
    fn test_split_meta_error() {
        let kv_err = KVAppError::MetaError(sample_meta_error());
        let result = kv_err.split();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MetaError::NetworkError(_)));
    }

    #[test]
    fn test_into_nested_ok() {
        let result: Result<i32, KVAppError> = Ok(42);
        let nested = result.into_nested();
        assert!(matches!(nested, Ok(Ok(42))));
    }

    #[test]
    fn test_into_nested_app_error() {
        let result: Result<i32, KVAppError> = Err(KVAppError::AppError(sample_app_error()));
        let nested = result.into_nested();
        assert!(nested.is_ok());
        let inner = nested.unwrap();
        assert!(inner.is_err());
        assert!(matches!(inner.unwrap_err(), AppError::TenantIsEmpty(_)));
    }

    #[test]
    fn test_into_nested_meta_error() {
        let result: Result<i32, KVAppError> = Err(KVAppError::MetaError(sample_meta_error()));
        let nested = result.into_nested();
        assert!(nested.is_err());
        assert!(matches!(nested.unwrap_err(), MetaError::NetworkError(_)));
    }

    #[test]
    fn test_from_nested_ok() {
        let nested: Result<Result<i32, AppError>, MetaError> = Ok(Ok(42));
        let flat = from_nested(nested);
        assert!(matches!(flat, Ok(42)));
    }

    #[test]
    fn test_from_nested_app_error() {
        let nested: Result<Result<i32, AppError>, MetaError> = Ok(Err(sample_app_error()));
        let flat = from_nested(nested);
        assert!(matches!(flat, Err(KVAppError::AppError(_))));
    }

    #[test]
    fn test_from_nested_meta_error() {
        let nested: Result<Result<i32, AppError>, MetaError> = Err(sample_meta_error());
        let flat = from_nested(nested);
        assert!(matches!(flat, Err(KVAppError::MetaError(_))));
    }

    #[test]
    fn test_round_trip_ok() {
        let original: Result<i32, KVAppError> = Ok(42);
        let nested = original.into_nested();
        let back = from_nested(nested);
        assert!(matches!(back, Ok(42)));
    }

    #[test]
    fn test_round_trip_app_error() {
        let original: Result<i32, KVAppError> = Err(KVAppError::AppError(sample_app_error()));
        let nested = original.into_nested();
        let back = from_nested(nested);
        assert!(matches!(back, Err(KVAppError::AppError(_))));
    }

    #[test]
    fn test_round_trip_meta_error() {
        let original: Result<i32, KVAppError> = Err(KVAppError::MetaError(sample_meta_error()));
        let nested = original.into_nested();
        let back = from_nested(nested);
        assert!(matches!(back, Err(KVAppError::MetaError(_))));
    }
}
