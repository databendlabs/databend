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

use crate::schema::TagObjectType;
use crate::schema::TaggableObject;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagRefName {
    pub object: TaggableObject,
    pub tag_name: String,
}

impl TagRefName {
    pub fn new(object: TaggableObject, tag_name: impl Into<String>) -> Self {
        Self {
            object,
            tag_name: tag_name.into(),
        }
    }
}

impl KeyCodec for TagRefName {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        let b = b.push_raw(self.object.object_type().as_str());
        let b = match &self.object {
            TaggableObject::Database { db_id } => b.push_u64(*db_id),
            TaggableObject::Table { db_id, table_id } => b.push_u64(*db_id).push_u64(*table_id),
            TaggableObject::Stage { name } => b.push_str(name),
            TaggableObject::Connection { name } => b.push_str(name),
        };
        b.push_str(&self.tag_name)
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
                TaggableObject::Database { db_id }
            }
            TagObjectType::Table => {
                let db_id = parser.next_u64()?;
                let table_id = parser.next_u64()?;
                TaggableObject::Table { db_id, table_id }
            }
            TagObjectType::Stage => {
                let name = parser.next_str()?;
                TaggableObject::Stage { name }
            }
            TagObjectType::Connection => {
                let name = parser.next_str()?;
                TaggableObject::Connection { name }
            }
        };

        let tag_name = parser.next_str()?;
        parser.done()?;
        Ok(Self { object, tag_name })
    }
}

/// Tag reference key `<tenant>/<object>/<tag_name>`.
pub type TagRefIdent = TIdent<Resource, TagRefName>;
pub type TagRefIdentRaw = TIdentRaw<Resource, TagRefName>;

pub use kvapi_impl::Resource;

mod kvapi_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::TagRefIdent;
    use crate::schema::TagRefValue;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_tag_ref";
        const TYPE: &'static str = "TagRefIdent";
        const HAS_TENANT: bool = true;
        type ValueType = TagRefValue;
    }

    impl kvapi::Value for TagRefValue {
        type KeyType = TagRefIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::TagRefIdent;
    use super::TagRefName;
    use crate::schema::TaggableObject;
    use crate::tenant::Tenant;

    #[test]
    fn test_tag_ref_ident_roundtrip() {
        let tenant = Tenant::new_literal("tenant_a");
        let name = TagRefName::new(
            TaggableObject::Table {
                db_id: 10,
                table_id: 22,
            },
            "sensitivity",
        );
        let ident = TagRefIdent::new_generic(tenant, name.clone());

        let key = ident.to_string_key();
        assert_eq!("__fd_tag_ref/tenant_a/table/10/22/sensitivity", key);
        assert_eq!(ident, TagRefIdent::from_str_key(&key).unwrap());
        assert_eq!(name, ident.name().clone());
    }
}
