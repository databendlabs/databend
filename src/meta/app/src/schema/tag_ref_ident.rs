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

use std::str::FromStr;

use databend_common_meta_kvapi::kvapi::KeyBuilder;
use databend_common_meta_kvapi::kvapi::KeyCodec;
use databend_common_meta_kvapi::kvapi::KeyError;
use databend_common_meta_kvapi::kvapi::KeyParser;

use crate::schema::TagObject;
use crate::schema::TagObjectType;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

/// Composite name identifying a tag-to-object binding.
///
/// Used as the "name" portion of [`TagRefIdent`] keys in the meta store.
/// The key format is `__fd_object_tag_ref/<tenant>/<object_type>/<object_id>/<tag_id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagRefName {
    /// The object this tag is attached to.
    pub object: TagObject,
    /// The ID of the tag.
    pub tag_id: u64,
}

impl TagRefName {
    pub fn new(object: TagObject, tag_id: u64) -> Self {
        Self { object, tag_id }
    }
}

impl KeyCodec for TagRefName {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        let b = b.push_raw(self.object.object_type().as_str());
        let b = match &self.object {
            TagObject::Database { db_id } => b.push_u64(*db_id),
            TagObject::Table { db_id, table_id } => b.push_u64(*db_id).push_u64(*table_id),
            TagObject::Stage { name } => b.push_str(name),
            TagObject::Connection { name } => b.push_str(name),
        };
        b.push_u64(self.tag_id)
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let raw_type = parser.next_raw()?.to_string();
        let object_type =
            TagObjectType::from_str(&raw_type).map_err(|_| KeyError::InvalidSegment {
                i: parser.index(),
                expect: "database|table|stage|connection".to_string(),
                got: raw_type.clone(),
            })?;

        let object = match object_type {
            TagObjectType::Database => {
                let db_id = parser.next_u64()?;
                TagObject::Database { db_id }
            }
            TagObjectType::Table => {
                let db_id = parser.next_u64()?;
                let table_id = parser.next_u64()?;
                TagObject::Table { db_id, table_id }
            }
            TagObjectType::Stage => {
                let name = parser.next_str()?;
                TagObject::Stage { name }
            }
            TagObjectType::Connection => {
                let name = parser.next_str()?;
                TagObject::Connection { name }
            }
        };

        let tag_id = parser.next_u64()?;
        parser.done()?;
        Ok(Self { object, tag_id })
    }
}

/// Tag reference key stored in meta-service (indexed by object).
///
/// Key format: `__fd_object_tag_ref/<tenant>/<object_type>/<object_id>/<tag_id>`
///
/// For tables, the object_id includes both `db_id` and `table_id` (e.g., `table/123/456/789`)
/// to allow efficient prefix scanning of all tags within a specific database.
pub type TagRefIdent = TIdent<Resource, TagRefName>;
pub type TagRefIdentRaw = TIdentRaw<Resource, TagRefName>;

pub use kvapi_impl::Resource;

/// Key format: `__fd_tag_object_ref/<tenant>/<tag_id>/<object_type>/<object_id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagRefObject {
    pub tag_id: u64,
    pub object: TagObject,
}

impl TagRefObject {
    pub fn new(tag_id: u64, object: TagObject) -> Self {
        Self { tag_id, object }
    }
}

impl KeyCodec for TagRefObject {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        let b = b.push_u64(self.tag_id);
        let b = b.push_raw(self.object.object_type().as_str());
        match &self.object {
            TagObject::Database { db_id } => b.push_u64(*db_id),
            TagObject::Table { db_id, table_id } => b.push_u64(*db_id).push_u64(*table_id),
            TagObject::Stage { name } => b.push_str(name),
            TagObject::Connection { name } => b.push_str(name),
        }
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let tag_id = parser.next_u64()?;
        let raw_type = parser.next_raw()?.to_string();
        let object_type =
            TagObjectType::from_str(&raw_type).map_err(|_| KeyError::InvalidSegment {
                i: parser.index(),
                expect: "database|table|stage|connection".to_string(),
                got: raw_type.clone(),
            })?;

        let object = match object_type {
            TagObjectType::Database => {
                let db_id = parser.next_u64()?;
                TagObject::Database { db_id }
            }
            TagObjectType::Table => {
                let db_id = parser.next_u64()?;
                let table_id = parser.next_u64()?;
                TagObject::Table { db_id, table_id }
            }
            TagObjectType::Stage => {
                let name = parser.next_str()?;
                TagObject::Stage { name }
            }
            TagObjectType::Connection => {
                let name = parser.next_str()?;
                TagObject::Connection { name }
            }
        };

        parser.done()?;
        Ok(Self { tag_id, object })
    }
}

pub type TagRefByIdIdent = TIdent<ByIdResource, TagRefObject>;
pub type TagRefByIdIdentRaw = TIdentRaw<ByIdResource, TagRefObject>;

pub use kvapi_impl::ByIdResource;

mod kvapi_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::EmptyProto;
    use crate::schema::TagRefByIdIdent;
    use crate::schema::TagRefIdent;
    use crate::schema::TagRefObjectValue;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_object_tag_ref";
        const TYPE: &'static str = "TagRefIdent";
        const HAS_TENANT: bool = true;
        type ValueType = TagRefObjectValue;
    }

    impl kvapi::Value for TagRefObjectValue {
        type KeyType = TagRefIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    pub struct ByIdResource;
    impl TenantResource for ByIdResource {
        const PREFIX: &'static str = "__fd_tag_object_ref";
        const TYPE: &'static str = "TagRefByIdIdent";
        const HAS_TENANT: bool = true;
        type ValueType = EmptyProto;
    }

    impl kvapi::Value for EmptyProto {
        type KeyType = TagRefByIdIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::TagRefByIdIdent;
    use super::TagRefIdent;
    use super::TagRefName;
    use super::TagRefObject;
    use crate::schema::TagObject;
    use crate::tenant::Tenant;

    #[test]
    fn test_tag_ref_ident_roundtrip() {
        let tenant = Tenant::new_literal("tenant_a");
        let name = TagRefName::new(
            TagObject::Table {
                db_id: 10,
                table_id: 22,
            },
            42, // tag_id
        );
        let ident = TagRefIdent::new_generic(tenant, name.clone());

        let key = ident.to_string_key();
        assert_eq!("__fd_object_tag_ref/tenant_a/table/10/22/42", key);
        assert_eq!(ident, TagRefIdent::from_str_key(&key).unwrap());
        assert_eq!(name, ident.name().clone());
    }

    #[test]
    fn test_tag_ref_by_id_ident_roundtrip() {
        let tenant = Tenant::new_literal("tenant_b");
        let name = TagRefObject::new(42, TagObject::Table {
            db_id: 10,
            table_id: 22,
        });
        let ident = TagRefByIdIdent::new_generic(tenant, name.clone());

        let key = ident.to_string_key();
        assert_eq!("__fd_tag_object_ref/tenant_b/42/table/10/22", key);
        assert_eq!(ident, TagRefByIdIdent::from_str_key(&key).unwrap());
        assert_eq!(name, ident.name().clone());
    }
}
