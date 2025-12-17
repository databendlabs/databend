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

pub mod id_ident;
pub mod id_to_name_ident;
pub mod name_ident;
pub mod ref_ident;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_types::SeqV;
pub use id_ident::TagId;
pub use id_ident::TagIdIdent;
pub use id_ident::TagIdIdentRaw;
pub use id_to_name_ident::TagIdToNameIdent;
pub use id_to_name_ident::TagIdToNameIdentRaw;
pub use name_ident::TagNameIdent;
pub use name_ident::TagNameIdentRaw;
pub use ref_ident::ObjectToTagId;
pub use ref_ident::ObjectToTagIdent;
pub use ref_ident::ObjectToTagIdentRaw;
pub use ref_ident::TagIdToObject;
pub use ref_ident::TagIdToObjectIdent;
pub use ref_ident::TagIdToObjectIdentRaw;
use serde::Deserialize;
use serde::Serialize;

use super::id_ident::Resource;
use crate::data_id::DataId;
use crate::tenant::Tenant;

/// Metadata stored for each tag definition.
///
/// Tags are user-defined labels that can be attached to Databend objects
/// (databases, tables, stages, connections) for governance and classification.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TagMeta {
    /// Optional list of allowed values for this tag. Mirrors the semantics of
    /// `CREATE TAG ... ALLOWED_VALUES`: declare before other options, accept up
    /// to 5,000 entries, and use order to resolve propagation conflicts. If
    /// unset, any string (including empty) is accepted when binding tags.
    pub allowed_values: Option<Vec<String>>,
    /// User-provided description of the tag.
    pub comment: String,
    pub created_on: DateTime<Utc>,
    pub updated_on: Option<DateTime<Utc>>,
}

/// Request to create a new tag definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTagReq {
    pub name_ident: TagNameIdent,
    pub meta: TagMeta,
}

/// Response from creating a tag.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TagInfo {
    pub name: String,
    pub tag_id: u64,
    pub meta: SeqV<TagMeta>,
}

/// Objects that can be tagged.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TagObject {
    Database { db_id: u64 },
    Table { table_id: u64 },
    Stage { name: String },
    Connection { name: String },
}

impl TagObject {
    pub fn type_str(&self) -> &'static str {
        match self {
            TagObject::Database { .. } => "database",
            TagObject::Table { .. } => "table",
            TagObject::Stage { .. } => "stage",
            TagObject::Connection { .. } => "connection",
        }
    }
}

/// Binds a set of values to tag IDs to a single object.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetObjectTagsReq {
    pub tenant: Tenant,
    pub object: TagObject,
    /// List of `(tag_id, tag_value)` pairs.
    pub tags: Vec<(u64, String)>,
}

/// Removes tag bindings from a single object by tag ID.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnsetObjectTagsReq {
    pub tenant: Tenant,
    pub object: TagObject,
    /// List of tag IDs to remove.
    pub tags: Vec<u64>,
}

/// Retrieves all tags bound to a single object.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GetObjectTagsReq {
    pub tenant: Tenant,
    pub object: TagObject,
}

/// Value stored for each object-to-tag binding in the meta store.
///
/// Stored at key [`ObjectToTagIdent`]: `__fd_object_tag_ref/<tenant>/<object_type>/<object_id>/<tag_id>`.
/// The `tag_id` is part of the key, so only the value payload and timestamp are stored here.
///
/// [`ObjectToTagIdent`]: crate::schema::ObjectToTagIdent
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ObjectToTagValue {
    /// Payload assigned when tagging an object. When [`TagMeta::allowed_values`]
    /// is present, this string must match one of the configured entries,
    /// otherwise any string (including empty) is allowed.
    pub value: String,
}

/// Value returned for each tag bound to an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectTagValue {
    pub tag_id: u64,
    /// The tag value with sequence number for optimistic concurrency control.
    pub tag_value: SeqV<ObjectToTagValue>,
}

/// Response carrying all tags and values assigned to the requested object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetObjectTagsReply {
    pub tags: Vec<ObjectTagValue>,
}

/// Lists all references for a tag by ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTagReferencesReq {
    pub tenant: Tenant,
    pub tag_id: u64,
}

/// Row returned from `list_tag_references`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagReferenceInfo {
    pub tag_id: u64,
    pub object: TagObject,
    /// The tag value with sequence number for optimistic concurrency control.
    pub tag_value: SeqV<ObjectToTagValue>,
}

/// Response packing the full list of references returned for a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTagReferencesReply {
    pub references: Vec<TagReferenceInfo>,
}
