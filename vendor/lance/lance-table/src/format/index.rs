// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Metadata for index

use std::sync::Arc;

use chrono::{DateTime, Utc};
use deepsize::DeepSizeOf;
use roaring::RoaringBitmap;
use snafu::location;
use uuid::Uuid;

use super::pb;
use lance_core::{Error, Result};

/// Index metadata
#[derive(Debug, Clone, PartialEq)]
pub struct IndexMetadata {
    /// Unique ID across all dataset versions.
    pub uuid: Uuid,

    /// Fields to build the index.
    pub fields: Vec<i32>,

    /// Human readable index name
    pub name: String,

    /// The version of the dataset this index was last updated on
    ///
    /// This is set when the index is created (based on the version used to train the index)
    /// This is updated when the index is updated or remapped
    pub dataset_version: u64,

    /// The fragment ids this index covers.
    ///
    /// This may contain fragment ids that no longer exist in the dataset.
    ///
    /// If this is None, then this is unknown.
    pub fragment_bitmap: Option<RoaringBitmap>,

    /// Metadata specific to the index type
    ///
    /// This is an Option because older versions of Lance may not have this defined.  However, it should always
    /// be present in newer versions.
    pub index_details: Option<Arc<prost_types::Any>>,

    /// The index version.
    pub index_version: i32,

    /// Timestamp when the index was created
    ///
    /// This field is optional for backward compatibility. For existing indices created before
    /// this field was added, this will be None.
    pub created_at: Option<DateTime<Utc>>,

    /// The base path index of the index files. Used when the index is imported or referred from another dataset.
    /// Lance uses it as key of the base_paths field in Manifest to determine the actual base path of the index files.
    pub base_id: Option<u32>,
}

impl IndexMetadata {
    pub fn effective_fragment_bitmap(
        &self,
        existing_fragments: &RoaringBitmap,
    ) -> Option<RoaringBitmap> {
        let fragment_bitmap = self.fragment_bitmap.as_ref()?;
        Some(fragment_bitmap & existing_fragments)
    }
}

impl DeepSizeOf for IndexMetadata {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.uuid.as_bytes().deep_size_of_children(context)
            + self.fields.deep_size_of_children(context)
            + self.name.deep_size_of_children(context)
            + self.dataset_version.deep_size_of_children(context)
            + self
                .fragment_bitmap
                .as_ref()
                .map(|fragment_bitmap| fragment_bitmap.serialized_size())
                .unwrap_or(0)
    }
}

impl TryFrom<pb::IndexMetadata> for IndexMetadata {
    type Error = Error;

    fn try_from(proto: pb::IndexMetadata) -> Result<Self> {
        let fragment_bitmap = if proto.fragment_bitmap.is_empty() {
            None
        } else {
            Some(RoaringBitmap::deserialize_from(
                &mut proto.fragment_bitmap.as_slice(),
            )?)
        };

        Ok(Self {
            uuid: proto.uuid.as_ref().map(Uuid::try_from).ok_or_else(|| {
                Error::io(
                    "uuid field does not exist in Index metadata".to_string(),
                    location!(),
                )
            })??,
            name: proto.name,
            fields: proto.fields,
            dataset_version: proto.dataset_version,
            fragment_bitmap,
            index_details: proto.index_details.map(Arc::new),
            index_version: proto.index_version.unwrap_or_default(),
            created_at: proto.created_at.map(|ts| {
                DateTime::from_timestamp_millis(ts as i64)
                    .expect("Invalid timestamp in index metadata")
            }),
            base_id: proto.base_id,
        })
    }
}

impl From<&IndexMetadata> for pb::IndexMetadata {
    fn from(idx: &IndexMetadata) -> Self {
        let mut fragment_bitmap = Vec::new();
        if let Some(bitmap) = &idx.fragment_bitmap {
            if let Err(e) = bitmap.serialize_into(&mut fragment_bitmap) {
                // In theory, this should never error. But if we do, just
                // recover gracefully.
                log::error!("Failed to serialize fragment bitmap: {}", e);
                fragment_bitmap.clear();
            }
        }

        Self {
            uuid: Some((&idx.uuid).into()),
            name: idx.name.clone(),
            fields: idx.fields.clone(),
            dataset_version: idx.dataset_version,
            fragment_bitmap,
            index_details: idx
                .index_details
                .as_ref()
                .map(|details| details.as_ref().clone()),
            index_version: Some(idx.index_version),
            created_at: idx.created_at.map(|dt| dt.timestamp_millis() as u64),
            base_id: idx.base_id,
        }
    }
}
