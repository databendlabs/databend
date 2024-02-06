use databend_common_meta_types::MetaError;

use crate::kv_pb_api::errors::PbDecodeError;
use crate::kv_pb_api::errors::PbEncodeError;

/// An error occurs when writing protobuf encoded value to kv store.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("PbApiWriteError: {0}")]
pub enum PbApiWriteError<E> {
    PbEncodeError(#[from] PbEncodeError),
    /// upsert reads the state transition after the operation.
    PbDecodeError(#[from] PbDecodeError),
    /// Error returned from KVApi.
    KvApiError(E),
}

impl From<PbApiWriteError<MetaError>> for MetaError {
    /// For KVApi that returns MetaError, convert protobuf related error to MetaError directly.
    fn from(value: PbApiWriteError<MetaError>) -> Self {
        match value {
            PbApiWriteError::PbEncodeError(e) => MetaError::from(e),
            PbApiWriteError::PbDecodeError(e) => MetaError::from(e),
            PbApiWriteError::KvApiError(e) => e,
        }
    }
}
