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

use crate::kv_pb_api::errors::NoneValue;
use crate::kv_pb_api::errors::PbApiReadError;
use crate::kv_pb_api::errors::PbDecodeError;
use crate::kv_pb_api::errors::PbEncodeError;

/// Encode an upsert Operation of T into protobuf encoded value.
pub fn encode_operation<T>(value: &Operation<T>) -> Result<Operation<Vec<u8>>, PbEncodeError>
where T: FromToProto {
    match value {
        Operation::Update(t) => {
            let p = t.to_pb()?;
            let mut buf = vec![];
            prost::Message::encode(&p, &mut buf)?;
            Ok(Operation::Update(buf))
        }
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
    let buf = &seqv.data;
    let x: Result<_, PbDecodeError> = try {
        let p: T::PB = prost::Message::decode(buf.as_ref()).map_err(PbDecodeError::from)?;
        let v: T = FromToProto::from_pb(p).map_err(PbDecodeError::from)?;
        SeqV::new_with_meta(seqv.seq, seqv.meta, v)
    };

    x.map_err(|e: PbDecodeError| e.with_context(context()))
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
