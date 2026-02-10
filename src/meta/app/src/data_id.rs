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

use databend_meta_kvapi::kvapi::KeyBuilder;
use databend_meta_kvapi::kvapi::KeyCodec;
use databend_meta_kvapi::kvapi::KeyError;
use databend_meta_kvapi::kvapi::KeyParser;
use derive_more::Deref;
use derive_more::DerefMut;

use crate::tenant::ToTenant;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::resource::TenantResource;

/// The identifier of an internal record used in an application upon [`kvapi::KVApi`],
/// e.g. TableId, DatabaseId as a value.
///
/// `DataId<R>` can be dereferenced to `u64`.
/// `DataId<R>` will take place of `Id<T>` in the future.
///
/// When an id is used as a key in a key-value store,
/// it is serialized in another way to keep order.
///
/// `DataId` implements `prost::Message` so that it can be used as a protobuf message.
/// Although internally it uses JSON encoding.
#[derive(Deref, DerefMut)]
pub struct DataId<R> {
    #[deref]
    #[deref_mut]
    id: u64,
    _p: PhantomData<R>,
}

impl<R> From<u64> for DataId<R> {
    fn from(id: u64) -> Self {
        Self {
            id,
            _p: PhantomData,
        }
    }
}

impl<R> fmt::Debug for DataId<R>
where R: TenantResource
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let type_name = self.type_name();

        f.debug_struct("DataId")
            .field("type", &type_name)
            .field("id", &self.id)
            .finish()
    }
}

impl<R> fmt::Display for DataId<R>
where R: TenantResource
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let type_name = self.type_name();

        write!(f, "DataId<{}>({})", type_name, self.id)
    }
}

impl<R> Clone for DataId<R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R> Copy for DataId<R> {}

impl<R> Default for DataId<R> {
    fn default() -> Self {
        Self::new(0)
    }
}

impl<R> PartialEq for DataId<R> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<R> Eq for DataId<R> {}

impl<R> DataId<R> {
    pub fn new(id: u64) -> Self {
        DataId {
            id,
            _p: PhantomData,
        }
    }

    /// Convert to a `TIdent` with the given tenant to form a complete key.
    pub fn into_t_ident(self, tenant: impl ToTenant) -> TIdent<R, Self> {
        TIdent::new_generic(tenant, self)
    }

    /// If there is a specified type name for this alias, use it.
    /// Otherwise, use the default name
    pub fn type_name(&self) -> &'static str
    where R: TenantResource {
        if R::TYPE.is_empty() {
            type_name::<R>().rsplit("::").next().unwrap_or("DataId")
        } else {
            R::TYPE
        }
    }
}

impl<R> KeyCodec for DataId<R> {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        b.push_u64(self.id)
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let v = parser.next_u64()?;
        Ok(Self::new(v))
    }
}

/// Implement `prost::Message` for `Id`, but internally use JSON encoding.
///
/// This enables `Id` to be used as a protobuf message,
/// so that the upper level code can be simplified to using just one trait `prost::Message` to access meta-service data.
mod prost_message_impl {

    use prost::DecodeError;
    use prost::bytes::Buf;
    use prost::bytes::BufMut;
    use prost::encoding::DecodeContext;
    use prost::encoding::WireType;

    use crate::data_id::DataId;
    use crate::tenant_key::resource::TenantResource;

    impl<R> prost::Message for DataId<R>
    where R: TenantResource + Sync + Send
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
                return Err(DecodeError::new(format!(
                    "buffer(len={}) is too large, max={}",
                    len,
                    b.len()
                )));
            }

            buf.copy_to_slice(&mut b[..len]);
            let id: u64 = serde_json::from_slice(&b[..len])
                .map_err(|e| DecodeError::new(format!("failed to decode u64 as json: {}", e)))?;
            Ok(DataId::new(id))
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

    #[cfg(test)]
    mod tests {

        use databend_meta_kvapi::kvapi;

        use crate::data_id::DataId;
        use crate::tenant_key::resource::TenantResource;

        struct Resource;
        impl TenantResource for Resource {
            const PREFIX: &'static str = "__fd_foo";
            const TYPE: &'static str = "Foo";
            const HAS_TENANT: bool = false;
            type ValueType = Bar;
        }

        #[derive(Debug, Default, PartialEq, Eq)]
        struct Foo(u64);

        impl kvapi::KeyCodec for Foo {
            fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
                b.push_u64(self.0)
            }

            fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
            where Self: Sized {
                let v = parser.next_u64()?;
                Ok(Foo(v))
            }
        }

        impl kvapi::Key for Foo {
            const PREFIX: &'static str = "";
            type ValueType = Bar;

            fn parent(&self) -> Option<String> {
                todo!()
            }
        }

        #[derive(Debug)]
        struct Bar;

        impl kvapi::Value for Bar {
            type KeyType = Foo;
            fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
                []
            }
        }

        #[test]
        fn test_id_as_protobuf_message() {
            let mut v: u64 = 1;
            for _i in 0..200 {
                v = v.wrapping_mul(7);

                let id = DataId::<Resource>::new(v);
                let mut buf = Vec::new();
                prost::Message::encode(&id, &mut buf).unwrap();
                let expected = format!("{}", v);
                assert_eq!(buf, expected.as_bytes());

                let id2: DataId<Resource> = prost::Message::decode(buf.as_ref()).unwrap();
                assert_eq!(id, id2);
            }
        }

        #[test]
        fn test_deref() {
            let id = DataId::<Resource>::new(1u64);
            assert_eq!(*id, 1);
        }

        #[test]
        fn test_debug() {
            let id = DataId::<Resource>::new(1u64);
            assert_eq!(format!("{:?}", id), r#"DataId { type: "Foo", id: 1 }"#);
        }

        #[test]
        fn test_display() {
            let id = DataId::<Resource>::new(1u64);
            assert_eq!(format!("{}", id), "DataId<Foo>(1)");
        }
    }
}
