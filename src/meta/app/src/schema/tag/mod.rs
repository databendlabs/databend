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
use databend_meta_kvapi::kvapi::KeyBuilder;
use databend_meta_kvapi::kvapi::KeyError;
use databend_meta_kvapi::kvapi::KeyParser;
use databend_meta_types::SeqV;
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaggableObject {
    Database { db_id: u64 },
    Table { table_id: u64 },
    Stage { name: String },
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
            TaggableObject::Connection { .. } => "connection",
            TaggableObject::UDF { .. } => "udf",
            TaggableObject::Procedure { .. } => "procedure",
        }
    }

    fn encode_to_key(&self, b: KeyBuilder) -> KeyBuilder {
        let b = b.push_raw(self.type_str());
        match self {
            TaggableObject::Database { db_id } => b.push_u64(*db_id),
            TaggableObject::Table { table_id } => b.push_u64(*table_id),
            TaggableObject::Stage { name } => b.push_str(name),
            TaggableObject::Connection { name } => b.push_str(name),
            TaggableObject::UDF { name } => b.push_str(name),
            TaggableObject::Procedure { name, args } => b.push_str(name).push_str(args),
        }
    }

    fn decode_from_key(parser: &mut KeyParser) -> Result<Self, KeyError> {
        let type_str = parser.next_raw()?;
        match type_str {
            "database" => Ok(TaggableObject::Database {
                db_id: parser.next_u64()?,
            }),
            "table" => Ok(TaggableObject::Table {
                table_id: parser.next_u64()?,
            }),
            "stage" => Ok(TaggableObject::Stage {
                name: parser.next_str()?,
            }),
            "connection" => Ok(TaggableObject::Connection {
                name: parser.next_str()?,
            }),
            "udf" => Ok(TaggableObject::UDF {
                name: parser.next_str()?,
            }),
            "procedure" => Ok(TaggableObject::Procedure {
                name: parser.next_str()?,
                args: parser.next_str()?,
            }),
            _ => Err(KeyError::InvalidSegment {
                i: parser.index(),
                expect: "database|table|stage|connection|udf|procedure".to_string(),
                got: type_str.to_string(),
            }),
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
