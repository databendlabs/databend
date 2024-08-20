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

use std::ops::Deref;

use databend_common_io::prelude::BufMut;
use databend_common_meta_kvapi::kvapi;
use prost::bytes::Buf;

/// The identifier of an internal record used in an application upon [`kvapi::KVApi`],
/// e.g. TableId, DatabaseId as a value.
///
/// When an id is used as a key in a key-value store,
/// it is serialized in another way to keep order.
///
/// `Id` implements `prost::Message` so that it can be used as a protobuf message.
/// Although internally it uses JSON encoding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id(pub u64);

impl Id {
    pub fn new(i: u64) -> Self {
        Id(i)
    }
}

/// Convert primitive u64 to Id.
impl From<u64> for Id {
    fn from(i: u64) -> Self {
        Id(i)
    }
}

/// Use `Id` as if using a `u64`
impl Deref for Id {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl kvapi::Value for Id {
    fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
        []
    }
}

/// Implement `prost::Message` for `Id`, but internally use JSON encoding.
///
/// This enables `Id` to be used as a protobuf message,
/// so that the upper level code can be simplified to using just one trait `prost::Message` to access meta-service data.
mod prost_message_impl {
    use prost::bytes::Buf;
    use prost::bytes::BufMut;
    use prost::encoding::DecodeContext;
    use prost::encoding::WireType;
    use prost::DecodeError;

    use crate::primitive::Id;

    impl prost::Message for Id {
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: BufMut,
            Self: Sized,
        {
            serde_json::to_writer(buf.writer(), &self.0).unwrap();
        }

        fn decode<B>(mut buf: B) -> Result<Self, DecodeError>
        where
            B: Buf,
            Self: Default,
        {
            let mut b = [0; 64];
            let len = buf.remaining();
            if len > b.len() {
                return Err(DecodeError::new(format!(
                    "buffer(len={}) is too large, max={}",
                    len,
                    b.len()
                )));
            }

            buf.copy_to_slice(&mut b[..len]);
            let id = serde_json::from_slice(&b[..len])
                .map_err(|e| DecodeError::new(format!("failed to decode u64 as json: {}", e)))?;
            Ok(Id::new(id))
        }

        fn merge_field<B>(
            &mut self,
            _tag: u32,
            _wire_type: WireType,
            _buf: &mut B,
            _ctx: DecodeContext,
        ) -> Result<(), DecodeError>
        where
            B: Buf,
            Self: Sized,
        {
            unimplemented!("`decode()` is implemented so we do not need this")
        }

        fn encoded_len(&self) -> usize {
            format!("{}", self.0).len()
        }

        fn clear(&mut self) {
            self.0 = 0;
        }
    }

    #[cfg(test)]
    mod tests {

        use crate::primitive::Id;

        #[test]
        fn test_id_as_protobuf_message() {
            let mut v: u64 = 1;
            for i in 0..200 {
                v = v.wrapping_mul(7);

                let id = Id(v);
                let mut buf = Vec::new();
                prost::Message::encode(&id, &mut buf).unwrap();
                let expected = format!("{}", v);
                assert_eq!(buf, expected.as_bytes());

                let id2: Id = prost::Message::decode(buf.as_ref()).unwrap();
                assert_eq!(id, id2);
            }
        }
    }
}
