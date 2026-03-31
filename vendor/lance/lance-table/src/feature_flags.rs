// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Feature flags

use snafu::location;

use crate::format::Manifest;
use lance_core::{Error, Result};

/// Fragments may contain deletion files, which record the tombstones of
/// soft-deleted rows.
pub const FLAG_DELETION_FILES: u64 = 1;
/// Row ids are stable for both moves and updates. Fragments contain an index
/// mapping row ids to row addresses.
pub const FLAG_STABLE_ROW_IDS: u64 = 2;
/// Files are written with the new v2 format (this flag is no longer used)
pub const FLAG_USE_V2_FORMAT_DEPRECATED: u64 = 4;
/// Table config is present
pub const FLAG_TABLE_CONFIG: u64 = 8;
/// Dataset uses multiple base paths (for shallow clones or multi-base datasets)
pub const FLAG_BASE_PATHS: u64 = 16;
/// Disable writing transaction file under _transaction/, this flag is set when we only want to write inline transaction in manifest
pub const FLAG_DISABLE_TRANSACTION_FILE: u64 = 32;
/// The first bit that is unknown as a feature flag
pub const FLAG_UNKNOWN: u64 = 64;

/// Set the reader and writer feature flags in the manifest based on the contents of the manifest.
pub fn apply_feature_flags(
    manifest: &mut Manifest,
    enable_stable_row_id: bool,
    disable_transaction_file: bool,
) -> Result<()> {
    // Reset flags
    manifest.reader_feature_flags = 0;
    manifest.writer_feature_flags = 0;

    let has_deletion_files = manifest
        .fragments
        .iter()
        .any(|frag| frag.deletion_file.is_some());
    if has_deletion_files {
        // Both readers and writers need to be able to read deletion files
        manifest.reader_feature_flags |= FLAG_DELETION_FILES;
        manifest.writer_feature_flags |= FLAG_DELETION_FILES;
    }

    // If any fragment has row ids, they must all have row ids.
    let has_row_ids = manifest
        .fragments
        .iter()
        .any(|frag| frag.row_id_meta.is_some());
    if has_row_ids || enable_stable_row_id {
        if !manifest
            .fragments
            .iter()
            .all(|frag| frag.row_id_meta.is_some())
        {
            return Err(Error::invalid_input(
                "All fragments must have row ids",
                location!(),
            ));
        }
        manifest.reader_feature_flags |= FLAG_STABLE_ROW_IDS;
        manifest.writer_feature_flags |= FLAG_STABLE_ROW_IDS;
    }

    // Test whether any table metadata has been set
    if !manifest.config.is_empty() {
        manifest.writer_feature_flags |= FLAG_TABLE_CONFIG;
    }

    // Check if this dataset uses multiple base paths (for shallow clones or multi-base datasets)
    if !manifest.base_paths.is_empty() {
        manifest.reader_feature_flags |= FLAG_BASE_PATHS;
        manifest.writer_feature_flags |= FLAG_BASE_PATHS;
    }

    if disable_transaction_file {
        manifest.writer_feature_flags |= FLAG_DISABLE_TRANSACTION_FILE;
    }
    Ok(())
}

pub fn can_read_dataset(reader_flags: u64) -> bool {
    reader_flags < FLAG_UNKNOWN
}

pub fn can_write_dataset(writer_flags: u64) -> bool {
    writer_flags < FLAG_UNKNOWN
}

pub fn has_deprecated_v2_feature_flag(writer_flags: u64) -> bool {
    writer_flags & FLAG_USE_V2_FORMAT_DEPRECATED != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::BasePath;

    #[test]
    fn test_read_check() {
        assert!(can_read_dataset(0));
        assert!(can_read_dataset(super::FLAG_DELETION_FILES));
        assert!(can_read_dataset(super::FLAG_STABLE_ROW_IDS));
        assert!(can_read_dataset(super::FLAG_USE_V2_FORMAT_DEPRECATED));
        assert!(can_read_dataset(super::FLAG_TABLE_CONFIG));
        assert!(can_read_dataset(super::FLAG_BASE_PATHS));
        assert!(can_read_dataset(super::FLAG_DISABLE_TRANSACTION_FILE));
        assert!(can_read_dataset(
            super::FLAG_DELETION_FILES
                | super::FLAG_STABLE_ROW_IDS
                | super::FLAG_USE_V2_FORMAT_DEPRECATED
        ));
        assert!(!can_read_dataset(super::FLAG_UNKNOWN));
    }

    #[test]
    fn test_write_check() {
        assert!(can_write_dataset(0));
        assert!(can_write_dataset(super::FLAG_DELETION_FILES));
        assert!(can_write_dataset(super::FLAG_STABLE_ROW_IDS));
        assert!(can_write_dataset(super::FLAG_USE_V2_FORMAT_DEPRECATED));
        assert!(can_write_dataset(super::FLAG_TABLE_CONFIG));
        assert!(can_write_dataset(super::FLAG_BASE_PATHS));
        assert!(can_write_dataset(super::FLAG_DISABLE_TRANSACTION_FILE));
        assert!(can_write_dataset(
            super::FLAG_DELETION_FILES
                | super::FLAG_STABLE_ROW_IDS
                | super::FLAG_USE_V2_FORMAT_DEPRECATED
                | super::FLAG_TABLE_CONFIG
                | super::FLAG_BASE_PATHS
        ));
        assert!(!can_write_dataset(super::FLAG_UNKNOWN));
    }

    #[test]
    fn test_base_paths_feature_flags() {
        use crate::format::{DataStorageFormat, Manifest};
        use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
        use lance_core::datatypes::Schema;
        use std::collections::HashMap;
        use std::sync::Arc;
        // Create a basic schema for testing
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "test_field",
            arrow_schema::DataType::Int64,
            false,
        )]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        // Test 1: Normal dataset (no base_paths) should not have FLAG_BASE_PATHS
        let mut normal_manifest = Manifest::new(
            schema.clone(),
            Arc::new(vec![]),
            DataStorageFormat::default(),
            HashMap::new(), // Empty base_paths
        );
        apply_feature_flags(&mut normal_manifest, false, false).unwrap();
        assert_eq!(normal_manifest.reader_feature_flags & FLAG_BASE_PATHS, 0);
        assert_eq!(normal_manifest.writer_feature_flags & FLAG_BASE_PATHS, 0);
        // Test 2: Dataset with base_paths (shallow clone or multi-base) should have FLAG_BASE_PATHS
        let mut base_paths: HashMap<u32, BasePath> = HashMap::new();
        base_paths.insert(
            1,
            BasePath::new(
                1,
                "file:///path/to/original".to_string(),
                Some("test_ref".to_string()),
                true,
            ),
        );
        let mut multi_base_manifest = Manifest::new(
            schema,
            Arc::new(vec![]),
            DataStorageFormat::default(),
            base_paths,
        );
        apply_feature_flags(&mut multi_base_manifest, false, false).unwrap();
        assert_ne!(
            multi_base_manifest.reader_feature_flags & FLAG_BASE_PATHS,
            0
        );
        assert_ne!(
            multi_base_manifest.writer_feature_flags & FLAG_BASE_PATHS,
            0
        );
    }
}
