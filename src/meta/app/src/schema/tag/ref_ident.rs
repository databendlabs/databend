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

use databend_meta_kvapi::kvapi::KeyBuilder;
use databend_meta_kvapi::kvapi::KeyCodec;
use databend_meta_kvapi::kvapi::KeyError;
use databend_meta_kvapi::kvapi::KeyParser;

use super::TaggableObject;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

/// Composite key component identifying an object-to-tag binding.
///
/// Here "object" refers to taggable Databend entities: Database, Table, Stage, or Connection.
///
/// Used as the "name" portion of [`ObjectTagIdRefIdent`] keys in the meta store.
/// The key format is `__fd_object_tag_ref/<tenant>/<object_type>/<object_id>/<tag_id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectTagIdRef {
    /// The object this tag is attached to.
    pub object: TaggableObject,
    /// The ID of the tag.
    pub tag_id: u64,
}

impl ObjectTagIdRef {
    pub fn new(object: TaggableObject, tag_id: u64) -> Self {
        Self { object, tag_id }
    }
}

impl KeyCodec for ObjectTagIdRef {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        self.object.encode_to_key(b).push_u64(self.tag_id)
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let object = TaggableObject::decode_from_key(parser)?;
        let tag_id = parser.next_u64()?;
        parser.done()?;
        Ok(Self { object, tag_id })
    }
}

/// Object -> tag reference key stored in meta-service (indexed by object).
///
/// Key format: `__fd_object_tag_ref/<tenant>/<object_type>/<object_id>/<tag_id>`
pub type ObjectTagIdRefIdent = TIdent<Resource, ObjectTagIdRef>;
pub type ObjectTagIdRefIdentRaw = TIdentRaw<Resource, ObjectTagIdRef>;

pub use kvapi_impl::Resource;

/// Composite key component for tag-to-object mapping.
///
/// Key format: `__fd_tag_object_ref/<tenant>/<tag_id>/<object_type>/<object_id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagIdObjectRef {
    pub tag_id: u64,
    pub object: TaggableObject,
}

impl TagIdObjectRef {
    pub fn new(tag_id: u64, object: TaggableObject) -> Self {
        Self { tag_id, object }
    }

    /// Create a prefix-only instance for directory listing.
    ///
    /// Used with `DirName::new_with_level(..., 1)` to scan all objects
    /// associated with a specific tag_id.
    pub fn prefix(tag_id: u64) -> Self {
        Self {
            tag_id,
            object: TaggableObject::Database { db_id: 0 },
        }
    }
}

impl KeyCodec for TagIdObjectRef {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        self.object.encode_to_key(b.push_u64(self.tag_id))
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let tag_id = parser.next_u64()?;
        let object = TaggableObject::decode_from_key(parser)?;
        parser.done()?;
        Ok(Self { tag_id, object })
    }
}

/// Tag -> object reference key stored in meta-service (indexed by tag).
///
/// Key format: `__fd_tag_object_ref/<tenant>/<tag_id>/<object_type>/<object_id>`
pub type TagIdObjectRefIdent = TIdent<TagIdObjectRefResource, TagIdObjectRef>;
pub type TagIdObjectRefIdentRaw = TIdentRaw<TagIdObjectRefResource, TagIdObjectRef>;

pub use kvapi_impl::TagIdObjectRefResource;

mod kvapi_impl {
    use databend_meta_kvapi::kvapi;

    use crate::schema::EmptyProto;
    use crate::schema::ObjectTagIdRefIdent;
    use crate::schema::ObjectTagIdRefValue;
    use crate::schema::TagIdObjectRefIdent;
    use crate::tenant_key::resource::TenantResource;

    /// Resource marker for object -> tag reference keys.
    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_object_tag_ref";
        const TYPE: &'static str = "ObjectTagIdRefIdent";
        const HAS_TENANT: bool = true;
        type ValueType = ObjectTagIdRefValue;
    }

    impl kvapi::Value for ObjectTagIdRefValue {
        type KeyType = ObjectTagIdRefIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    /// Resource marker for tag -> object reference keys.
    pub struct TagIdObjectRefResource;
    impl TenantResource for TagIdObjectRefResource {
        const PREFIX: &'static str = "__fd_tag_object_ref";
        const TYPE: &'static str = "TagIdObjectRefIdent";
        const HAS_TENANT: bool = true;
        type ValueType = EmptyProto;
    }

    impl kvapi::Value for EmptyProto {
        type KeyType = TagIdObjectRefIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::ObjectTagIdRef;
    use super::ObjectTagIdRefIdent;
    use super::TagIdObjectRef;
    use super::TagIdObjectRefIdent;
    use super::TaggableObject;
    use crate::tenant::Tenant;

    #[test]
    fn test_object_tag_id_ref_ident() {
        let tenant = Tenant::new_literal("tenant_a");
        let name = ObjectTagIdRef::new(TaggableObject::Table { table_id: 22 }, 42);
        let ident = ObjectTagIdRefIdent::new_generic(tenant, name.clone());

        let key = ident.to_string_key();
        assert_eq!("__fd_object_tag_ref/tenant_a/table/22/42", key);
        assert_eq!(ident, ObjectTagIdRefIdent::from_str_key(&key).unwrap());
        assert_eq!(name, ident.name().clone());
    }

    #[test]
    fn test_tag_id_object_ref_ident() {
        let tenant = Tenant::new_literal("tenant_b");
        let name = TagIdObjectRef::new(42, TaggableObject::Table { table_id: 22 });
        let ident = TagIdObjectRefIdent::new_generic(tenant, name.clone());

        let key = ident.to_string_key();
        assert_eq!("__fd_tag_object_ref/tenant_b/42/table/22", key);
        assert_eq!(ident, TagIdObjectRefIdent::from_str_key(&key).unwrap());
        assert_eq!(name, ident.name().clone());
    }
}
