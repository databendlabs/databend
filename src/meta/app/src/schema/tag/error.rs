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

use databend_common_exception::ErrorCode;

use crate::schema::tag::TagId;
use crate::schema::tag::TaggableObject;
use crate::schema::tag::id_ident::Resource as TagIdResource;
use crate::tenant::Tenant;
use crate::tenant_key::errors::UnknownError;

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum TagError {
    #[error("TAG `{tag_name}` not found.")]
    TagNotFound { tag_name: String },

    #[error(
        "TAG `{tag_name}` is still referenced by {reference_count} object(s): {references_display}. Remove the references before dropping it."
    )]
    TagHasReferences {
        tag_name: String,
        reference_count: usize,
        references_display: String,
    },
}

impl TagError {
    pub fn tag_has_references(tag_name: String, references: Vec<String>) -> Self {
        let reference_count = references.len();
        // Show up to 5 references to avoid overly long error messages
        let references_display = if references.len() <= 5 {
            references.join(", ")
        } else {
            format!(
                "{}, ... and {} more",
                references[..5].join(", "),
                references.len() - 5
            )
        };
        Self::TagHasReferences {
            tag_name,
            reference_count,
            references_display,
        }
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum TagMetaError {
    #[error(transparent)]
    UnknownTagId(#[from] UnknownError<TagIdResource, TagId>),

    #[error("Invalid value '{tag_value}' for TAG with id {tag_id}. {allowed_values_display}.")]
    NotAllowedValue {
        tag_id: u64,
        tag_value: String,
        allowed_values_display: String,
    },

    #[error("{} does not exist.", .0)]
    ObjectNotFound(TaggableObject),
}

impl TagMetaError {
    pub fn not_found(tenant: &Tenant, tag_id: u64) -> Self {
        let ident = TagId::new(tag_id).into_t_ident(tenant);
        Self::UnknownTagId(ident.unknown_error("tag id not found"))
    }

    pub fn not_allowed_value(
        tag_id: u64,
        tag_value: impl Into<String>,
        allowed_values: Vec<String>,
    ) -> Self {
        let allowed_values_display = if allowed_values.is_empty() {
            "This tag currently does not allow any values.".to_string()
        } else {
            format!(
                "Allowed values: [{}]",
                allowed_values
                    .iter()
                    .map(|v| format!("'{v}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        Self::NotAllowedValue {
            tag_id,
            tag_value: tag_value.into(),
            allowed_values_display,
        }
    }

    pub fn object_not_found(object: TaggableObject) -> Self {
        Self::ObjectNotFound(object)
    }
}

impl From<TagError> for ErrorCode {
    fn from(value: TagError) -> Self {
        let s = value.to_string();
        match value {
            TagError::TagNotFound { .. } => ErrorCode::UnknownTag(s),
            TagError::TagHasReferences { .. } => ErrorCode::TagHasReferences(s),
        }
    }
}

impl From<TagMetaError> for ErrorCode {
    fn from(err: TagMetaError) -> Self {
        let s = err.to_string();
        match err {
            TagMetaError::UnknownTagId(err) => ErrorCode::from(err),
            TagMetaError::NotAllowedValue { .. } => ErrorCode::NotAllowedTagValue(s),
            TagMetaError::ObjectNotFound(ref obj) => match obj {
                TaggableObject::Connection { .. } => ErrorCode::UnknownConnection(s),
                TaggableObject::Stage { .. } => ErrorCode::UnknownStage(s),
                TaggableObject::Database { .. } => ErrorCode::UnknownDatabase(s),
                TaggableObject::Table { .. } => ErrorCode::UnknownTable(s),
                TaggableObject::UDF { .. } => ErrorCode::UnknownFunction(s),
                TaggableObject::Procedure { .. } => ErrorCode::UnknownProcedure(s),
            },
        }
    }
}

impl From<UnknownError<TagIdResource, TagId>> for ErrorCode {
    fn from(err: UnknownError<TagIdResource, TagId>) -> Self {
        ErrorCode::UnknownTag(err.to_string())
    }
}
