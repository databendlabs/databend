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

use databend_common_proto_conv::FromToProto;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::NonEmptyItem;
use databend_meta_types::Change;
use databend_meta_types::Operation;
use databend_meta_types::SeqV;
use databend_meta_types::protobuf::StreamItem;

use super::compress;
use crate::kv_pb_api::errors::NoneValue;
use crate::kv_pb_api::errors::PbApiReadError;
use crate::kv_pb_api::errors::PbDecodeError;
use crate::kv_pb_api::errors::PbEncodeError;

/// Encode a `FromToProto` value to protobuf bytes, with transparent zstd compression.
pub fn encode_pb<T: FromToProto>(value: &T) -> Result<Vec<u8>, PbEncodeError> {
    let p = value.to_pb()?;
    let mut buf = vec![];
    prost::Message::encode(&p, &mut buf)?;
    Ok(compress::encode_value(buf))
}

/// Decode protobuf bytes (possibly zstd-compressed) to a `FromToProto` value.
pub fn decode_pb<T: FromToProto>(buf: &[u8]) -> Result<T, PbDecodeError> {
    let buf = compress::decode_value(buf)
        .map_err(|e| PbDecodeError::from(prost::DecodeError::new(e.to_string())))?;
    let p: T::PB = prost::Message::decode(buf.as_ref()).map_err(PbDecodeError::from)?;
    T::from_pb(p).map_err(PbDecodeError::from)
}

/// Encode an upsert Operation of T into protobuf encoded value.
pub fn encode_operation<T>(value: &Operation<T>) -> Result<Operation<Vec<u8>>, PbEncodeError>
where T: FromToProto {
    match value {
        Operation::Update(t) => Ok(Operation::Update(encode_pb(t)?)),
        Operation::Delete => Ok(Operation::Delete),
        _ => {
            unreachable!("Operation::AsIs is not supported")
        }
    }
}

/// Decode Change<Vec<u8>> into Change<T>, with FromToProto.
pub fn decode_change<T>(change: Change<Vec<u8>>) -> Result<Change<T>, PbDecodeError>
where T: FromToProto {
    let prev = change
        .prev
        .map(|seqv| {
            decode_seqv::<T>(seqv, || {
                format!("decode `prev` value of {:?}", change.ident)
            })
        })
        .transpose()?;

    let result = change
        .result
        .map(|seqv| {
            decode_seqv::<T>(seqv, || {
                format!("decode `result` value of {:?}", change.ident)
            })
        })
        .transpose()?;

    let c = Change {
        ident: change.ident,
        prev,
        result,
    };

    Ok(c)
}

/// Deserialize SeqV<Vec<u8>> into SeqV<T>, with FromToProto.
///
/// A context function is used to provide a context for error messages, when decoding fails.
pub fn decode_seqv<T>(
    seqv: SeqV,
    context: impl FnOnce() -> String,
) -> Result<SeqV<T>, PbDecodeError>
where
    T: FromToProto,
{
    let v = decode_pb::<T>(seqv.data.as_ref()).map_err(|e| e.with_context(context()))?;
    Ok(SeqV::new_with_meta(seqv.seq, seqv.meta, v))
}

/// Decode key and protobuf encoded value from `StreamItem`.
///
/// It requires K to be static because it is used in a static stream map()
pub fn decode_non_empty_item<K, E>(
    r: Result<StreamItem, E>,
) -> Result<NonEmptyItem<K>, PbApiReadError<E>>
where
    K: kvapi::Key + 'static,
    K::ValueType: FromToProto,
{
    match r {
        Ok(item) => {
            let k = K::from_str_key(&item.key)?;

            let raw = item.value.ok_or_else(|| NoneValue::new(item.key))?;
            let v = decode_seqv::<K::ValueType>(SeqV::from(raw), || {
                format!("decode value of {}", k.to_string_key())
            })?;

            Ok(NonEmptyItem::new(k, v))
        }
        Err(e) => Err(PbApiReadError::KvApiError(e)),
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Utc;
    use databend_common_meta_app::schema::CatalogMeta;
    use databend_common_meta_app::schema::CatalogOption;
    use databend_common_meta_app::schema::HiveCatalogOption;

    use super::*;
    use crate::kv_pb_api::compress::COMPRESS_THRESHOLD;

    /// Large values written via `encode_pb` (the path used by `upsert_pb`) must be
    /// stored with the `[0x0F, FLAG_ZSTD, 0x00, 0x00]` header so that the metaservice
    /// holds them in compressed form.
    #[test]
    fn test_large_value_is_stored_compressed() {
        let large_address = "x".repeat(COMPRESS_THRESHOLD + 1024);
        let meta = CatalogMeta {
            catalog_option: CatalogOption::Hive(HiveCatalogOption {
                address: large_address,
                storage_params: None,
            }),
            created_on: DateTime::<Utc>::MIN_UTC,
        };

        let encoded = encode_pb(&meta).unwrap();

        assert!(encoded.len() >= 4, "encoded value must be at least 4 bytes");
        assert_eq!(
            &encoded[..4],
            &[0x0F, 0x01, 0x00, 0x00],
            "large values must start with the zstd compression header [0x0F, FLAG_ZSTD, 0, 0]"
        );

        let decoded: CatalogMeta = decode_pb(&encoded).unwrap();
        assert_eq!(decoded, meta, "round-trip must recover the original value");
    }

    /// Small values must be stored as raw protobuf (no compression header).
    #[test]
    fn test_small_value_is_not_compressed() {
        let meta = CatalogMeta {
            catalog_option: CatalogOption::Hive(HiveCatalogOption {
                address: "127.0.0.1:10000".to_string(),
                storage_params: None,
            }),
            created_on: DateTime::<Utc>::MIN_UTC,
        };

        let encoded = encode_pb(&meta).unwrap();

        assert_ne!(
            encoded.first().copied(),
            Some(0x0F),
            "small values must not have the compression header"
        );

        let decoded: CatalogMeta = decode_pb(&encoded).unwrap();
        assert_eq!(decoded, meta, "round-trip must recover the original value");
    }
}
