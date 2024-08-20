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

use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;

/// A wrapper implements Deref<Target=u64>, which is used as the default type for `T` in `Id<T>`.
pub struct U64(u64);

impl Deref for U64 {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for U64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<u64> for U64 {
    fn from(id: u64) -> Self {
        U64(id)
    }
}

/// The identifier of an internal record used in an application upon [`kvapi::KVApi`],
/// e.g. TableId, DatabaseId as a value.
///
/// `Id<T>` can be dereferenced to `u64`.
///
/// When an id is used as a key in a key-value store,
/// it is serialized in another way to keep order.
///
/// `Id` implements `prost::Message` so that it can be used as a protobuf message.
/// Although internally it uses JSON encoding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id<T = U64> {
    id: T,
}

impl Id<U64> {
    pub fn new(id: u64) -> Self {
        Id { id: U64(id) }
    }
}

impl<T> Id<T> {
    pub fn new_typed(id: T) -> Self {
        Id { id }
    }

    pub fn inner(&self) -> &T {
        &self.id
    }
}

/// Convert primitive u64 to Id.
impl<T> From<u64> for Id<T>
where T: From<u64>
{
    fn from(id: u64) -> Self {
        Id { id: T::from(id) }
    }
}

/// Use `Id` as if using a `u64`
impl<T> Deref for Id<T>
where T: Deref<Target = u64>
{
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        self.id.deref()
    }
}

impl<T> fmt::Display for Id<T>
where T: Deref<Target = u64>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_name = std::any::type_name::<T>();
        let last = type_name.rsplitn(2, "::").next().unwrap_or(type_name);
        write!(f, "Id({}({}))", last, *self.id)
    }
}

/// Implement `prost::Message` for `Id`, but internally use JSON encoding.
///
/// This enables `Id` to be used as a protobuf message,
/// so that the upper level code can be simplified to using just one trait `prost::Message` to access meta-service data.
mod prost_message_impl {
    use std::fmt;
    use std::ops::Deref;
    use std::ops::DerefMut;

    use prost::bytes::Buf;
    use prost::bytes::BufMut;
    use prost::encoding::DecodeContext;
    use prost::encoding::WireType;
    use prost::DecodeError;

    use crate::primitive::Id;

    impl<T> prost::Message for Id<T>
    where
        T: fmt::Debug + Send + Sync,
        T: From<u64> + Deref<Target = u64> + DerefMut<Target = u64>,
    {
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: BufMut,
            Self: Sized,
        {
            serde_json::to_writer(buf.writer(), &self.id.deref()).unwrap();
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
            let id: u64 = serde_json::from_slice(&b[..len])
                .map_err(|e| DecodeError::new(format!("failed to decode u64 as json: {}", e)))?;
            Ok(Id::from(id))
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
            format!("{}", self.id.deref()).len()
        }

        fn clear(&mut self) {
            *self.id.deref_mut() = 0;
        }
    }

    #[cfg(test)]
    mod tests {
        use std::ops::Deref;
        use std::ops::DerefMut;

        use crate::primitive::Id;

        #[derive(Debug, Default, PartialEq, Eq)]
        struct Foo(u64);

        impl From<u64> for Foo {
            fn from(id: u64) -> Self {
                Foo(id)
            }
        }

        impl Deref for Foo {
            type Target = u64;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl DerefMut for Foo {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        #[test]
        fn test_id_as_protobuf_message() {
            let mut v: u64 = 1;
            for _i in 0..200 {
                v = v.wrapping_mul(7);

                let id = Id::new_typed(Foo(v));
                let mut buf = Vec::new();
                prost::Message::encode(&id, &mut buf).unwrap();
                let expected = format!("{}", v);
                assert_eq!(buf, expected.as_bytes());

                let id2: Id<Foo> = prost::Message::decode(buf.as_ref()).unwrap();
                assert_eq!(id, id2);
            }
        }

        #[test]
        fn test_deref() {
            let id = Id::new(1u64);
            assert_eq!(*id, 1);
        }

        #[test]
        fn test_display() {
            let id = Id::new(1u64);
            assert_eq!(format!("{}", id), "Id(U64(1))");

            let id = Id::new_typed(Foo(1));
            assert_eq!(format!("{}", id), "Id(Foo(1))");
        }
    }
}
