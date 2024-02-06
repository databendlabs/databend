use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaError;

use crate::kv_pb_api::errors::PbDecodeError;

/// An error occurs when found an unexpected None value.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("NoneValue: unexpected None value of key: '{key}'")]
pub struct NoneValue {
    key: String,
}

impl NoneValue {
    pub fn new(key: impl ToString) -> Self {
        NoneValue {
            key: key.to_string(),
        }
    }
}

/// An error occurs when reading protobuf encoded value from kv store.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("PbApiReadError: {0}")]
pub enum PbApiReadError<E> {
    PbDecodeError(#[from] PbDecodeError),
    KeyError(#[from] kvapi::KeyError),
    NoneValue(#[from] NoneValue),
    /// Error returned from KVApi.
    KvApiError(E),
}

impl From<PbApiReadError<MetaError>> for MetaError {
    /// For KVApi that returns MetaError, convert protobuf related error to MetaError directly.
    ///
    /// Because MetaError contains network protocol level error variant.
    /// If there is a decoding error, consider it as network level error.
    fn from(value: PbApiReadError<MetaError>) -> Self {
        match value {
            PbApiReadError::PbDecodeError(e) => MetaError::from(e),
            PbApiReadError::KeyError(e) => MetaError::from(InvalidReply::new("", &e)),
            PbApiReadError::NoneValue(e) => MetaError::from(InvalidReply::new("", &e)),
            PbApiReadError::KvApiError(e) => e,
        }
    }
}
