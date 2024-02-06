use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaError;
use databend_common_proto_conv::Incompatible;

/// An error occurred when decoding protobuf encoded value.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("PbDecodeError: {0}")]
pub enum PbDecodeError {
    DecodeError(#[from] prost::DecodeError),
    Incompatible(#[from] Incompatible),
}

impl From<PbDecodeError> for MetaError {
    fn from(value: PbDecodeError) -> Self {
        match value {
            PbDecodeError::DecodeError(e) => MetaError::from(InvalidReply::new("", &e)),
            PbDecodeError::Incompatible(e) => MetaError::from(InvalidReply::new("", &e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::kv_pb_api::errors::PbDecodeError;

    #[test]
    fn test_error_message() {
        let e = PbDecodeError::DecodeError(prost::DecodeError::new("decode error"));
        assert_eq!(
            "PbDecodeError: failed to decode Protobuf message: decode error",
            e.to_string()
        );
    }
}
