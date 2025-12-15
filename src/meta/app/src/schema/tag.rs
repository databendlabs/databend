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
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

use crate::schema::CreateOption;
use crate::schema::TagNameIdent;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;

/// Metadata stored for each tag definition.
///
/// Tags are user-defined labels that can be attached to Databend objects
/// (databases, tables, stages, connections) for governance and classification.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TagMeta {
    /// Optional list of allowed values for this tag.
    /// If set, only these values can be assigned when tagging objects.
    pub allowed_values: Option<Vec<String>>,
    /// User-provided description of the tag.
    pub comment: String,
    pub created_on: DateTime<Utc>,
    pub updated_on: Option<DateTime<Utc>>,
}

/// Request to create a new tag definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTagReq {
    pub create_option: CreateOption,
    pub name_ident: TagNameIdent,
    pub meta: TagMeta,
}

/// Response from creating a tag.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateTagReply {
    pub tag_id: u64,
}

/// Request to drop (delete) a tag definition.
///
/// Dropping a tag will fail if it is still referenced by any objects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTagReq {
    pub if_exists: bool,
    pub name_ident: TagNameIdent,
}

/// Request to retrieve a tag definition by name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTagReq {
    pub name_ident: TagNameIdent,
}

/// Response containing tag metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTagReply {
    pub tag_id: u64,
    pub meta: TagMeta,
}

/// Request to list all tags for a tenant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTagsReq {
    pub tenant: Tenant,
}

impl ListTagsReq {
    pub fn new(tenant: impl ToTenant) -> Self {
        Self {
            tenant: tenant.to_tenant(),
        }
    }
}

/// Complete information about a tag, including its name, ID, and metadata.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TagInfo {
    pub name: String,
    pub tag_id: u64,
    pub meta: TagMeta,
}

/// Response containing a list of tags.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListTagsReply {
    pub tags: Vec<TagInfo>,
}

/// Types of objects that can have tags attached.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagObjectType {
    Database,
    Table,
    Stage,
    Connection,
}

impl TagObjectType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TagObjectType::Database => "database",
            TagObjectType::Table => "table",
            TagObjectType::Stage => "stage",
            TagObjectType::Connection => "connection",
        }
    }
}

impl Display for TagObjectType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TagObjectType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "database" => Ok(TagObjectType::Database),
            "table" => Ok(TagObjectType::Table),
            "stage" => Ok(TagObjectType::Stage),
            "connection" => Ok(TagObjectType::Connection),
            _ => Err(()),
        }
    }
}

/// Objects that can be tagged in V1.
///
/// Stage/Connection do not expose stable numeric ids, so we persist their names
/// inside the reference keys instead.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TaggableObject {
    Database { db_id: u64 },
    Table { db_id: u64, table_id: u64 },
    Stage { name: String },
    Connection { name: String },
}

impl TaggableObject {
    pub fn object_type(&self) -> TagObjectType {
        match self {
            TaggableObject::Database { .. } => TagObjectType::Database,
            TaggableObject::Table { .. } => TagObjectType::Table,
            TaggableObject::Stage { .. } => TagObjectType::Stage,
            TaggableObject::Connection { .. } => TagObjectType::Connection,
        }
    }

    pub fn object_id(&self) -> String {
        match self {
            TaggableObject::Database { db_id } => db_id.to_string(),
            TaggableObject::Table { db_id, table_id } => format!("{}/{}", db_id, table_id),
            TaggableObject::Stage { name } => name.clone(),
            TaggableObject::Connection { name } => name.clone(),
        }
    }
}

/// Value stored for each tag-to-object binding in the meta store.
///
/// Stored at key [`TagRefIdent`]: `__fd_tag_ref/<tenant>/<object_type>/<object_id>/<tag_name>`.
///
/// [`TagRefIdent`]: crate::schema::TagRefIdent
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TagRefValue {
    /// The ID of the tag definition this reference points to.
    pub tag_id: u64,
    /// User-supplied payload for the tag, e.g. a classification label.
    /// When the tag definition defined `allowed_values`, this must be one of them.
    pub value: String,
    pub created_on: DateTime<Utc>,
}

/// Binds a set of values to tag names for a single object.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetObjectTagsReq {
    pub tenant: Tenant,
    pub object: TaggableObject,
    pub tags: Vec<(String, String)>,
}

/// Removes the given tag names from a single object.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnsetObjectTagsReq {
    pub tenant: Tenant,
    pub object: TaggableObject,
    pub tags: Vec<String>,
}

/// Retrieves all tags bound to a single object.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GetObjectTagsReq {
    pub tenant: Tenant,
    pub object: TaggableObject,
}

/// Value returned for each tag bound to an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectTagValue {
    pub tag_name: String,
    pub tag_id: u64,
    /// String payload that was assigned to `tag_name` for this object.
    /// Mirrors [`TagRefValue::value`] and must respect the tag's allowed values.
    pub tag_value: String,
    pub created_on: DateTime<Utc>,
}

/// Response carrying all tags and values assigned to the requested object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetObjectTagsReply {
    pub tags: Vec<ObjectTagValue>,
}

/// Lists all references for an optional tag/object-type filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTagReferencesReq {
    pub tenant: Tenant,
    pub tag_name: Option<String>,
    pub object_type: Option<TagObjectType>,
}

/// Row returned from `list_tag_references`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagReferenceInfo {
    pub object_type: TagObjectType,
    pub object_id: String,
    pub tag_name: String,
    pub tag_value: String,
    pub created_on: DateTime<Utc>,
}

/// Response packing the full list of references returned for a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTagReferencesReply {
    pub references: Vec<TagReferenceInfo>,
}
