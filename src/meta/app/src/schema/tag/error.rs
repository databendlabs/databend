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

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum TagError {
    /// Keep a domain error here instead of reusing `UnknownError` so callers can
    /// map it back to `ErrorCode::UnknownTag` without converting through the
    /// tenant-key helpers.
    #[error("TAG with id {tag_id} does not exist.")]
    NotFound { tag_id: u64 },

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

    #[error("Tag metadata was modified concurrently, please retry (e.g., allowed_values changed)")]
    ConcurrentModification,
}

impl TagError {
    pub fn not_found(tag_id: u64) -> Self {
        Self::NotFound { tag_id }
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

    pub fn concurrent_modification() -> Self {
        Self::ConcurrentModification
    }
}

impl From<TagError> for ErrorCode {
    fn from(value: TagError) -> Self {
        let s = value.to_string();
        match value {
            TagError::NotFound { .. } => ErrorCode::UnknownTag(s),
            TagError::TagHasReferences { .. } => ErrorCode::TagHasReferences(s),
            TagError::NotAllowedValue { .. } => ErrorCode::NotAllowedTagValue(s),
            TagError::ConcurrentModification => ErrorCode::TagConcurrentModification(s),
        }
    }
}
