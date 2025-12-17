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

use databend_common_meta_kvapi::kvapi::KeyBuilder;
use databend_common_meta_kvapi::kvapi::KeyCodec;
use databend_common_meta_kvapi::kvapi::KeyError;
use databend_common_meta_kvapi::kvapi::KeyParser;

use super::TagObject;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

// Helper methods for TagObject key encoding/decoding
impl TagObject {
    fn encode_to_key(&self, b: KeyBuilder) -> KeyBuilder {
        let b = b.push_raw(self.type_str());
        match self {
            TagObject::Database { db_id } => b.push_u64(*db_id),
            TagObject::Table { table_id } => b.push_u64(*table_id),
            TagObject::Stage { name } => b.push_str(name),
            TagObject::Connection { name } => b.push_str(name),
        }
    }

    fn decode_from_key(parser: &mut KeyParser) -> Result<Self, KeyError> {
        let type_str = parser.next_raw()?;
        match type_str {
            "database" => Ok(TagObject::Database {
                db_id: parser.next_u64()?,
            }),
            "table" => Ok(TagObject::Table {
                table_id: parser.next_u64()?,
            }),
            "stage" => Ok(TagObject::Stage {
                name: parser.next_str()?,
            }),
            "connection" => Ok(TagObject::Connection {
                name: parser.next_str()?,
            }),
            _ => Err(KeyError::InvalidSegment {
                i: parser.index(),
                expect: "database|table|stage|connection".to_string(),
                got: type_str.to_string(),
            }),
        }
    }
}

/// Composite key component identifying an object-to-tag binding.
///
/// Here "object" refers to taggable Databend entities: Database, Table, Stage, or Connection.
///
/// Used as the "name" portion of [`ObjectToTagIdent`] keys in the meta store.
/// The key format is `__fd_object_tag_ref/<tenant>/<object_type>/<object_id>/<tag_id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectToTagId {
    /// The object this tag is attached to.
    pub object: TagObject,
    /// The ID of the tag.
    pub tag_id: u64,
}

impl ObjectToTagId {
    pub fn new(object: TagObject, tag_id: u64) -> Self {
        Self { object, tag_id }
    }
}

impl KeyCodec for ObjectToTagId {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        self.object.encode_to_key(b).push_u64(self.tag_id)
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let object = TagObject::decode_from_key(parser)?;
        let tag_id = parser.next_u64()?;
        parser.done()?;
        Ok(Self { object, tag_id })
    }
}

/// Object-to-tag reference key stored in meta-service (indexed by object).
///
/// Key format: `__fd_object_tag_ref/<tenant>/<object_type>/<object_id>/<tag_id>`
pub type ObjectToTagIdent = TIdent<ObjectToTagResource, ObjectToTagId>;
pub type ObjectToTagIdentRaw = TIdentRaw<ObjectToTagResource, ObjectToTagId>;

pub use kvapi_impl::ObjectToTagResource;

/// Composite key component for tag-to-object mapping.
///
/// Key format: `__fd_tag_object_ref/<tenant>/<tag_id>/<object_type>/<object_id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagIdToObject {
    pub tag_id: u64,
    pub object: TagObject,
}

impl TagIdToObject {
    pub fn new(tag_id: u64, object: TagObject) -> Self {
        Self { tag_id, object }
    }

    /// Create a prefix-only instance for directory listing.
    ///
    /// Used with `DirName::new_with_level(..., 1)` to scan all objects
    /// associated with a specific tag_id.
    pub fn prefix(tag_id: u64) -> Self {
        Self {
            tag_id,
            object: TagObject::Database { db_id: 0 },
        }
    }
}

impl KeyCodec for TagIdToObject {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        self.object.encode_to_key(b.push_u64(self.tag_id))
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let tag_id = parser.next_u64()?;
        let object = TagObject::decode_from_key(parser)?;
        parser.done()?;
        Ok(Self { tag_id, object })
    }
}

/// Tag-to-object reference key stored in meta-service (indexed by tag).
///
/// Key format: `__fd_tag_object_ref/<tenant>/<tag_id>/<object_type>/<object_id>`
pub type TagIdToObjectIdent = TIdent<TagIdToObjectResource, TagIdToObject>;
pub type TagIdToObjectIdentRaw = TIdentRaw<TagIdToObjectResource, TagIdToObject>;

pub use kvapi_impl::TagIdToObjectResource;

mod kvapi_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::EmptyProto;
    use crate::schema::ObjectToTagIdent;
    use crate::schema::ObjectToTagValue;
    use crate::schema::TagIdToObjectIdent;
    use crate::tenant_key::resource::TenantResource;

    /// Resource marker for object-to-tag reference keys.
    pub struct ObjectToTagResource;
    impl TenantResource for ObjectToTagResource {
        const PREFIX: &'static str = "__fd_object_tag_ref";
        const TYPE: &'static str = "ObjectToTagIdent";
        const HAS_TENANT: bool = true;
        type ValueType = ObjectToTagValue;
    }

    impl kvapi::Value for ObjectToTagValue {
        type KeyType = ObjectToTagIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    /// Resource marker for tag-to-object reference keys.
    pub struct TagIdToObjectResource;
    impl TenantResource for TagIdToObjectResource {
        const PREFIX: &'static str = "__fd_tag_object_ref";
        const TYPE: &'static str = "TagIdToObjectIdent";
        const HAS_TENANT: bool = true;
        type ValueType = EmptyProto;
    }

    impl kvapi::Value for EmptyProto {
        type KeyType = TagIdToObjectIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::ObjectToTagId;
    use super::ObjectToTagIdent;
    use super::TagIdToObject;
    use super::TagIdToObjectIdent;
    use super::TagObject;
    use crate::tenant::Tenant;

    #[test]
    fn test_object_to_tag_ident_roundtrip() {
        let tenant = Tenant::new_literal("tenant_a");
        let name = ObjectToTagId::new(TagObject::Table { table_id: 22 }, 42);
        let ident = ObjectToTagIdent::new_generic(tenant, name.clone());

        let key = ident.to_string_key();
        assert_eq!("__fd_object_tag_ref/tenant_a/table/22/42", key);
        assert_eq!(ident, ObjectToTagIdent::from_str_key(&key).unwrap());
        assert_eq!(name, ident.name().clone());
    }

    #[test]
    fn test_tag_id_to_object_ident_roundtrip() {
        let tenant = Tenant::new_literal("tenant_b");
        let name = TagIdToObject::new(42, TagObject::Table { table_id: 22 });
        let ident = TagIdToObjectIdent::new_generic(tenant, name.clone());

        let key = ident.to_string_key();
        assert_eq!("__fd_tag_object_ref/tenant_b/42/table/22", key);
        assert_eq!(ident, TagIdToObjectIdent::from_str_key(&key).unwrap());
        assert_eq!(name, ident.name().clone());
    }
}
