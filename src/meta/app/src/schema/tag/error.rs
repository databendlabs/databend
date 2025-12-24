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
use crate::schema::tag::id_ident::Resource as TagIdResource;
use crate::schema::tag::id_ident::TagIdIdent;
use crate::tenant::Tenant;
use crate::tenant_key::errors::UnknownError;

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum TagError {
    #[error(transparent)]
    UnknownTagId(#[from] UnknownError<TagIdResource, TagId>),

    #[error(
        "TAG `{tag_name}` is still referenced by {reference_count} object(s). Remove the references before dropping it."
    )]
    TagHasReferences {
        tag_name: String,
        reference_count: usize,
    },

    #[error("Invalid value '{tag_value}' for TAG with id {tag_id}{allowed_values_display}.")]
    NotAllowedValue {
        tag_id: u64,
        tag_value: String,
        allowed_values_display: String,
    },

    #[error(
        "Tag metadata for tag id(s) {tag_ids:?} was modified concurrently, please retry (e.g., allowed_values changed)"
    )]
    TagMetaConcurrentModification { tag_ids: Vec<u64> },
}

impl TagError {
    pub fn not_found(tenant: &Tenant, tag_id: u64) -> Self {
        let ident = TagId::new(tag_id).into_t_ident(tenant);
        Self::UnknownTagId(ident.unknown_error("tag id not found"))
    }

    pub fn not_found_ident(tag_ident: TagIdIdent) -> Self {
        Self::UnknownTagId(tag_ident.unknown_error("tag id not found"))
    }

    pub fn tag_has_references(tag_name: impl Into<String>, reference_count: usize) -> Self {
        Self::TagHasReferences {
            tag_name: tag_name.into(),
            reference_count,
        }
    }

    pub fn not_allowed_value(
        tag_id: u64,
        tag_value: impl Into<String>,
        allowed_values: Option<Vec<String>>,
    ) -> Self {
        let allowed_values_display = match allowed_values {
            Some(values) if !values.is_empty() => format!(
                ". Allowed values: [{}]",
                values
                    .into_iter()
                    .map(|v| format!("'{v}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            _ => String::new(),
        };

        Self::NotAllowedValue {
            tag_id,
            tag_value: tag_value.into(),
            allowed_values_display,
        }
    }

    pub fn concurrent_modification(tag_ids: impl Into<Vec<u64>>) -> Self {
        Self::TagMetaConcurrentModification {
            tag_ids: tag_ids.into(),
        }
    }
}

impl From<TagError> for ErrorCode {
    fn from(value: TagError) -> Self {
        let s = value.to_string();
        match value {
            TagError::UnknownTagId(err) => ErrorCode::from(err),
            TagError::TagHasReferences { .. } => ErrorCode::TagHasReferences(s),
            TagError::NotAllowedValue { .. } => ErrorCode::NotAllowedTagValue(s),
            TagError::TagMetaConcurrentModification { .. } => {
                ErrorCode::TagMetaConcurrentModification(s)
            }
        }
    }
}

impl From<UnknownError<TagIdResource, TagId>> for ErrorCode {
    fn from(err: UnknownError<TagIdResource, TagId>) -> Self {
        ErrorCode::UnknownTag(err.to_string())
    }
}
