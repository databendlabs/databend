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
use std::fmt;
use std::marker::PhantomData;

use derive_more::Deref;
use derive_more::DerefMut;

/// A typed `u64` that is only used as a meta-service value.
///
/// `ValueId<T>` stores the integer directly. `T` is only a marker that keeps
/// values from different storage records from being mixed accidentally.
#[derive(Deref, DerefMut)]
pub struct ValueId<T> {
    #[deref]
    #[deref_mut]
    id: u64,
    _p: PhantomData<T>,
}

impl<T> From<u64> for ValueId<T> {
    fn from(id: u64) -> Self {
        Self {
            id,
            _p: PhantomData,
        }
    }
}

impl<T> fmt::Debug for ValueId<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ValueId")
            .field("type", &self.type_name())
            .field("id", &self.id)
            .finish()
    }
}

impl<T> fmt::Display for ValueId<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ValueId<{}>({})", self.type_name(), self.id)
    }
}

impl<T> Clone for ValueId<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ValueId<T> {}

impl<T> Default for ValueId<T> {
    fn default() -> Self {
        Self::new(0)
    }
}

impl<T> PartialEq for ValueId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for ValueId<T> {}

impl<T> ValueId<T> {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            _p: PhantomData,
        }
    }

    pub fn type_name(&self) -> &'static str {
        type_name::<T>().rsplit("::").next().unwrap_or("ValueId")
    }
}

mod prost_message_impl {
    use prost::DecodeError;
    use prost::bytes::Buf;
    use prost::bytes::BufMut;
    use prost::encoding::DecodeContext;
    use prost::encoding::WireType;

    use crate::value_id::ValueId;

    #[allow(deprecated)]
    fn decode_error(message: impl Into<String>) -> DecodeError {
        DecodeError::new(message.into())
    }

    impl<T> prost::Message for ValueId<T>
    where T: Sync + Send
    {
        fn encode_raw(&self, buf: &mut impl BufMut)
        where Self: Sized {
            serde_json::to_writer(buf.writer(), &self.id).unwrap();
        }

        fn decode(mut buf: impl Buf) -> Result<Self, DecodeError>
        where Self: Default {
            let mut b = [0; 64];
            let len = buf.remaining();
            if len > b.len() {
                return Err(decode_error(format!(
                    "buffer(len={}) is too large, max={}",
                    len,
                    b.len()
                )));
            }

            buf.copy_to_slice(&mut b[..len]);
            let id: u64 = serde_json::from_slice(&b[..len])
                .map_err(|e| decode_error(format!("failed to decode u64 as json: {}", e)))?;
            Ok(ValueId::new(id))
        }

        fn merge_field(
            &mut self,
            _tag: u32,
            _wire_type: WireType,
            _buf: &mut impl Buf,
            _ctx: DecodeContext,
        ) -> Result<(), DecodeError>
        where
            Self: Sized,
        {
            unimplemented!("`decode()` is implemented so we do not need this")
        }

        fn encoded_len(&self) -> usize {
            format!("{}", self.id).len()
        }

        fn clear(&mut self) {
            self.id = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::value_id::ValueId;

    struct Resource;

    fn assert_pb_round_trip(value: u64) {
        let id = ValueId::<Resource>::new(value);
        let buf = prost::Message::encode_to_vec(&id);

        let expected = value.to_string();
        assert_eq!(buf, expected.as_bytes());
        assert_eq!(prost::Message::encoded_len(&id), expected.len());

        let decoded: ValueId<Resource> = prost::Message::decode(buf.as_ref()).unwrap();
        assert_eq!(decoded, id);
    }

    #[test]
    fn test_value_id_as_protobuf_message() {
        let mut v: u64 = 1;
        for _i in 0..200 {
            v = v.wrapping_mul(7);

            assert_pb_round_trip(v);
        }
    }

    #[test]
    fn test_value_id_as_protobuf_message_boundary_values() {
        for value in [
            0,
            1,
            u8::MAX as u64,
            u16::MAX as u64,
            u32::MAX as u64,
            i64::MAX as u64,
            u64::MAX - 1,
            u64::MAX,
        ] {
            assert_pb_round_trip(value);
        }
    }

    #[test]
    fn test_value_id_protobuf_decode_rejects_invalid_u64() {
        for (input, want) in [
            (
                "",
                "failed to decode Protobuf message: failed to decode u64 as json: EOF while parsing a value at line 1 column 0",
            ),
            (
                "-1",
                "failed to decode Protobuf message: failed to decode u64 as json: invalid value: integer `-1`, expected u64 at line 1 column 2",
            ),
            (
                "1.5",
                "failed to decode Protobuf message: failed to decode u64 as json: invalid type: floating point `1.5`, expected u64 at line 1 column 3",
            ),
            (
                "18446744073709551616",
                "failed to decode Protobuf message: failed to decode u64 as json: invalid type: floating point `1.8446744073709552e+19`, expected u64 at line 1 column 20",
            ),
            (
                "not-json",
                "failed to decode Protobuf message: failed to decode u64 as json: expected ident at line 1 column 2",
            ),
        ] {
            let got = <ValueId<Resource> as prost::Message>::decode(input.as_bytes())
                .unwrap_err()
                .to_string();
            assert_eq!(got, want);
        }
    }

    #[test]
    fn test_value_id_protobuf_decode_rejects_oversized_buffer() {
        let input = vec![b'1'; 65];
        let got = <ValueId<Resource> as prost::Message>::decode(input.as_slice())
            .unwrap_err()
            .to_string();

        assert_eq!(
            got,
            "failed to decode Protobuf message: buffer(len=65) is too large, max=64"
        );
    }

    #[test]
    fn test_value_id_protobuf_clear() {
        let mut id = ValueId::<Resource>::new(u64::MAX);
        prost::Message::clear(&mut id);
        assert_eq!(*id, 0);
    }

    #[test]
    fn test_deref() {
        let id = ValueId::<Resource>::new(1u64);
        assert_eq!(*id, 1);
    }

    #[test]
    fn test_debug() {
        let id = ValueId::<Resource>::new(1u64);
        assert_eq!(
            format!("{:?}", id),
            r#"ValueId { type: "Resource", id: 1 }"#
        );
    }

    #[test]
    fn test_display() {
        let id = ValueId::<Resource>::new(1u64);
        assert_eq!(format!("{}", id), "ValueId<Resource>(1)");
    }
}
