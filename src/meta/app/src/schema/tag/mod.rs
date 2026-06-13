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

pub mod error;
pub mod id_ident;
pub mod id_to_name_ident;
pub mod name_ident;
pub mod ref_ident;

use chrono::DateTime;
use chrono::Utc;
use databend_meta_client::kvapi;
use databend_meta_client::types::SeqV;
pub use error::TagError;
pub use id_ident::TagId;
pub use id_ident::TagIdIdent;
pub use id_ident::TagIdIdentRaw;
pub use id_to_name_ident::TagIdToNameIdent;
pub use id_to_name_ident::TagIdToNameIdentRaw;
pub use name_ident::TagNameIdent;
pub use name_ident::TagNameIdentRaw;
pub use ref_ident::ObjectTagIdRef;
pub use ref_ident::ObjectTagIdRefIdent;
pub use ref_ident::ObjectTagIdRefIdentRaw;
pub use ref_ident::TagIdObjectRef;
pub use ref_ident::TagIdObjectRefIdent;
pub use ref_ident::TagIdObjectRefIdentRaw;

use crate::data_id::DataId;
use crate::principal::UserIdentity;
use crate::schema::tag::id_ident::Resource;
use crate::tenant::Tenant;

/// Metadata stored for each tag definition.
///
/// Tags are user-defined labels that can be attached to Databend objects
/// (databases, tables, stages, connections) for governance and classification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagMeta {
    /// Ordered list mirroring `CREATE TAG ... ALLOWED_VALUES`. Always preserves
    /// declaration order so ON_CONFLICT = ALLOWED_VALUES_SEQUENCE can
    /// deterministically pick winners. Duplicates are removed during creation.
    /// When `Some`, the contained vector may be empty (meaning the constraint
    /// exists but currently rejects all values). When `None`, the tag accepts
    /// any string.
    pub allowed_values: Option<Vec<String>>,
    /// User-provided description of the tag.
    pub comment: String,
    pub created_on: DateTime<Utc>,
    pub updated_on: Option<DateTime<Utc>>,
    /// When Some, indicates the tag has been dropped (soft delete timestamp).
    /// This is reserved for future UNDROP/vacuum semantics; currently unused.
    pub drop_on: Option<DateTime<Utc>>,
}

/// Request to create a new tag definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTagReq {
    pub name_ident: TagNameIdent,
    pub meta: TagMeta,
}

/// Response from creating a tag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTagReply {
    pub tag_id: u64,
}

/// Response containing tag metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTagReply {
    pub tag_id: SeqV<DataId<Resource>>,
    pub meta: SeqV<TagMeta>,
}

/// Complete information about a tag, including its name, ID, and metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagInfo {
    pub name: String,
    /// Sequence-wrapped tag id, so callers can observe name->id mapping changes.
    pub tag_id: SeqV<TagId>,
    pub meta: SeqV<TagMeta>,
}

/// Objects that can be tagged.
#[derive(Clone, Debug, PartialEq, Eq, kvapi::KeyCodec)]
pub enum TaggableObject {
    Database { db_id: u64 },
    Table { table_id: u64 },
    Stage { name: String },
    User { user: UserIdentity },
    Role { name: String },
    Connection { name: String },
    UDF { name: String },
    Procedure { name: String, args: String },
}

impl std::fmt::Display for TaggableObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaggableObject::Database { db_id } => write!(f, "database(id={})", db_id),
            TaggableObject::Table { table_id } => write!(f, "table(id={})", table_id),
            TaggableObject::Stage { name } => write!(f, "stage({})", name),
            TaggableObject::User { user } => write!(f, "user({})", user.display()),
            TaggableObject::Role { name } => write!(f, "role({})", name),
            TaggableObject::Connection { name } => write!(f, "connection({})", name),
            TaggableObject::UDF { name } => write!(f, "udf({})", name),
            TaggableObject::Procedure { name, args } => {
                write!(f, "procedure({}({}))", name, args)
            }
        }
    }
}

impl TaggableObject {
    pub fn type_str(&self) -> &'static str {
        match self {
            TaggableObject::Database { .. } => "database",
            TaggableObject::Table { .. } => "table",
            TaggableObject::Stage { .. } => "stage",
            TaggableObject::User { .. } => "user",
            TaggableObject::Role { .. } => "role",
            TaggableObject::Connection { .. } => "connection",
            TaggableObject::UDF { .. } => "udf",
            TaggableObject::Procedure { .. } => "procedure",
        }
    }
}

/// Binds a set of values to tag IDs to a single taggable object.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetObjectTagsReq {
    pub tenant: Tenant,
    pub taggable_object: TaggableObject,
    /// List of `(tag_id, tag_value)` pairs.
    pub tags: Vec<(u64, String)>,
}

/// Removes tag bindings from a single taggable object by tag ID.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnsetObjectTagsReq {
    pub tenant: Tenant,
    pub taggable_object: TaggableObject,
    /// List of tag IDs to remove.
    pub tags: Vec<u64>,
}

/// Value stored for each object-to-tag binding in the meta store.
///
/// Stored at key [`ObjectToTagIdent`]: `__fd_object_tag_ref/<tenant>/<object_type>/<object_id>/<tag_id>`.
/// The `tag_id` is part of the key, so only the value payload and timestamp are stored here.
///
/// [`ObjectToTagIdent`]: crate::schema::ObjectTagIdRefIdent
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectTagIdRefValue {
    /// Payload assigned when tagging an object. When [`TagMeta::allowed_values`]
    /// is `Some`, this string must match one of the configured entries,
    /// otherwise any string (including empty) is allowed.
    pub tag_allowed_value: String,
}

/// Value returned for each tag bound to an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectTagValue {
    pub tag_id: u64,
    /// The tag value with sequence number for optimistic concurrency control.
    pub tag_value: SeqV<ObjectTagIdRefValue>,
}

/// Row returned from `list_tag_references`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagReferenceInfo {
    pub tag_id: u64,
    pub taggable_object: TaggableObject,
    /// The tag value with sequence number for optimistic concurrency control.
    pub tag_value: SeqV<ObjectTagIdRefValue>,
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi;
    use databend_meta_client::kvapi::KeyCodec;

    use super::TaggableObject;
    use crate::principal::UserIdentity;

    /// Encodes `obj`, asserts the resulting key matches `expected`,
    /// confirms `segment_count()` matches the actual segment count, and
    /// verifies the round-trip back to the original value. Wire-format
    /// changes here are breaking — every variant that ships a key into
    /// the meta-store needs a snapshot.
    fn round_trip(obj: TaggableObject, expected: &str) {
        let encoded = obj.encode_key(kvapi::KeyBuilder::new()).done();
        assert_eq!(expected, encoded, "encode mismatch for {:?}", obj);

        // segment_count must equal the actual number of `/`-separated
        // segments. A mismatch would cause `to_string_key` (which uses
        // segment_count to size the Builder budget) to silently truncate.
        assert_eq!(
            expected.split('/').count(),
            obj.segment_count(),
            "segment_count mismatch for {:?}",
            obj,
        );

        let mut p = kvapi::KeyParser::new(&encoded);
        let decoded = TaggableObject::decode_key(&mut p).expect("decode");
        assert_eq!(obj, decoded, "round-trip mismatch");
    }

    #[test]
    fn test_taggable_object_key_format() {
        round_trip(TaggableObject::Database { db_id: 7 }, "database/7");
        round_trip(TaggableObject::Table { table_id: 42 }, "table/42");
        round_trip(
            TaggableObject::Stage {
                name: "my_stage".to_string(),
            },
            "stage/my_stage",
        );
        // `UserIdentity::encode` wraps username/host in single quotes
        // joined by `@`; `push_str` then percent-escapes those literals
        // (`'` -> `%27`, `@` -> `%40`).
        round_trip(
            TaggableObject::User {
                user: UserIdentity::new("alice", "localhost"),
            },
            "user/%27alice%27%40%27localhost%27",
        );
        round_trip(
            TaggableObject::Role {
                name: "admin".to_string(),
            },
            "role/admin",
        );
        round_trip(
            TaggableObject::Connection {
                name: "s3conn".to_string(),
            },
            "connection/s3conn",
        );
        round_trip(
            TaggableObject::UDF {
                name: "my_udf".to_string(),
            },
            "udf/my_udf",
        );
        // Procedure pushes 3 segments (tag + name + args). The `,` in
        // `args` escapes to `%2c`, confirming the field still routes
        // through `push_str`.
        round_trip(
            TaggableObject::Procedure {
                name: "my_proc".to_string(),
                args: "u32,String".to_string(),
            },
            "procedure/my_proc/u32%2cString",
        );
    }

    #[test]
    fn test_taggable_object_decode_unknown_tag() {
        let mut p = kvapi::KeyParser::new("nonsuch/123");
        let err = TaggableObject::decode_key(&mut p).unwrap_err();
        assert!(
            matches!(err, kvapi::KeyError::InvalidSegment { .. }),
            "expected InvalidSegment, got {:?}",
            err,
        );
    }
}
