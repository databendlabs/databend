// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::num::NonZero;

use deepsize::DeepSizeOf;
use lance_core::Error;
use lance_file::format::{MAJOR_VERSION, MINOR_VERSION};
use lance_file::version::LanceFileVersion;
use lance_io::utils::CachedFileSize;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use snafu::location;

use crate::format::pb;

use crate::rowids::version::{
    created_at_version_meta_to_pb, last_updated_at_version_meta_to_pb, RowDatasetVersionMeta,
};
use lance_core::datatypes::Schema;
use lance_core::error::Result;

/// Lance Data File
///
/// A data file is one piece of file storing data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct DataFile {
    /// Relative path of the data file to dataset root.
    pub path: String,
    /// The ids of fields in this file.
    pub fields: Vec<i32>,
    /// The offsets of the fields listed in `fields`, empty in v1 files
    ///
    /// Note that -1 is a possibility and it indices that the field has
    /// no top-level column in the file.
    ///
    /// Columns that lack a field id may still exist as extra entries in
    /// `column_indices`; such columns are ignored by field-id–based projection.
    /// For example, some fields, such as blob fields, occupy multiple
    /// columns in the file but only have a single field id.
    #[serde(default)]
    pub column_indices: Vec<i32>,
    /// The major version of the file format used to write this file.
    #[serde(default)]
    pub file_major_version: u32,
    /// The minor version of the file format used to write this file.
    #[serde(default)]
    pub file_minor_version: u32,

    /// The size of the file in bytes, if known.
    pub file_size_bytes: CachedFileSize,

    /// The base path of the datafile, when the datafile is outside the dataset.
    pub base_id: Option<u32>,
}

impl DataFile {
    pub fn new(
        path: impl Into<String>,
        fields: Vec<i32>,
        column_indices: Vec<i32>,
        file_major_version: u32,
        file_minor_version: u32,
        file_size_bytes: Option<NonZero<u64>>,
        base_id: Option<u32>,
    ) -> Self {
        Self {
            path: path.into(),
            fields,
            column_indices,
            file_major_version,
            file_minor_version,
            file_size_bytes: file_size_bytes.into(),
            base_id,
        }
    }

    /// Create a new `DataFile` with the expectation that fields and column_indices will be set later
    pub fn new_unstarted(
        path: impl Into<String>,
        file_major_version: u32,
        file_minor_version: u32,
    ) -> Self {
        Self {
            path: path.into(),
            fields: vec![],
            column_indices: vec![],
            file_major_version,
            file_minor_version,
            file_size_bytes: Default::default(),
            base_id: None,
        }
    }

    pub fn new_legacy_from_fields(
        path: impl Into<String>,
        fields: Vec<i32>,
        base_id: Option<u32>,
    ) -> Self {
        Self::new(
            path,
            fields,
            vec![],
            MAJOR_VERSION as u32,
            MINOR_VERSION as u32,
            None,
            base_id,
        )
    }

    pub fn new_legacy(
        path: impl Into<String>,
        schema: &Schema,
        file_size_bytes: Option<NonZero<u64>>,
        base_id: Option<u32>,
    ) -> Self {
        let mut field_ids = schema.field_ids();
        field_ids.sort();
        Self::new(
            path,
            field_ids,
            vec![],
            MAJOR_VERSION as u32,
            MINOR_VERSION as u32,
            file_size_bytes,
            base_id,
        )
    }

    pub fn schema(&self, full_schema: &Schema) -> Schema {
        full_schema.project_by_ids(&self.fields, false)
    }

    pub fn is_legacy_file(&self) -> bool {
        self.file_major_version == 0 && self.file_minor_version < 3
    }

    pub fn validate(&self, base_path: &Path) -> Result<()> {
        if self.is_legacy_file() {
            if !self.fields.windows(2).all(|w| w[0] < w[1]) {
                return Err(Error::corrupt_file(
                    base_path.child(self.path.clone()),
                    "contained unsorted or duplicate field ids",
                    location!(),
                ));
            }
        } else if self.column_indices.len() < self.fields.len() {
            // Every recorded field id must have a column index, but not every column needs
            // to be associated with a field id (extra columns are allowed).
            return Err(Error::corrupt_file(
                base_path.child(self.path.clone()),
                "contained fewer column_indices than fields",
                location!(),
            ));
        }
        Ok(())
    }
}

impl From<&DataFile> for pb::DataFile {
    fn from(df: &DataFile) -> Self {
        Self {
            path: df.path.clone(),
            fields: df.fields.clone(),
            column_indices: df.column_indices.clone(),
            file_major_version: df.file_major_version,
            file_minor_version: df.file_minor_version,
            file_size_bytes: df.file_size_bytes.get().map_or(0, |v| v.get()),
            base_id: df.base_id,
        }
    }
}

impl TryFrom<pb::DataFile> for DataFile {
    type Error = Error;

    fn try_from(proto: pb::DataFile) -> Result<Self> {
        Ok(Self {
            path: proto.path,
            fields: proto.fields,
            column_indices: proto.column_indices,
            file_major_version: proto.file_major_version,
            file_minor_version: proto.file_minor_version,
            file_size_bytes: CachedFileSize::new(proto.file_size_bytes),
            base_id: proto.base_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
#[serde(rename_all = "lowercase")]
pub enum DeletionFileType {
    Array,
    Bitmap,
}

impl DeletionFileType {
    // TODO: pub(crate)
    pub fn suffix(&self) -> &str {
        match self {
            Self::Array => "arrow",
            Self::Bitmap => "bin",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct DeletionFile {
    pub read_version: u64,
    pub id: u64,
    pub file_type: DeletionFileType,
    /// Number of deleted rows in this file. If None, this is unknown.
    pub num_deleted_rows: Option<usize>,
    pub base_id: Option<u32>,
}

impl TryFrom<pb::DeletionFile> for DeletionFile {
    type Error = Error;

    fn try_from(value: pb::DeletionFile) -> Result<Self> {
        let file_type = match value.file_type {
            0 => DeletionFileType::Array,
            1 => DeletionFileType::Bitmap,
            _ => {
                return Err(Error::NotSupported {
                    source: "Unknown deletion file type".into(),
                    location: location!(),
                })
            }
        };
        let num_deleted_rows = if value.num_deleted_rows == 0 {
            None
        } else {
            Some(value.num_deleted_rows as usize)
        };
        Ok(Self {
            read_version: value.read_version,
            id: value.id,
            file_type,
            num_deleted_rows,
            base_id: value.base_id,
        })
    }
}

/// A reference to a part of a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct ExternalFile {
    pub path: String,
    pub offset: u64,
    pub size: u64,
}

/// Metadata about location of the row id sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub enum RowIdMeta {
    Inline(Vec<u8>),
    External(ExternalFile),
}

impl TryFrom<pb::data_fragment::RowIdSequence> for RowIdMeta {
    type Error = Error;

    fn try_from(value: pb::data_fragment::RowIdSequence) -> Result<Self> {
        match value {
            pb::data_fragment::RowIdSequence::InlineRowIds(data) => Ok(Self::Inline(data)),
            pb::data_fragment::RowIdSequence::ExternalRowIds(file) => {
                Ok(Self::External(ExternalFile {
                    path: file.path.clone(),
                    offset: file.offset,
                    size: file.size,
                }))
            }
        }
    }
}

/// Data fragment.
///
/// A fragment is a set of files which represent the different columns of the same rows.
/// If column exists in the schema, but the related file does not exist, treat this column as `nulls`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct Fragment {
    /// Fragment ID
    pub id: u64,

    /// Files within the fragment.
    pub files: Vec<DataFile>,

    /// Optional file with deleted local row offsets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_file: Option<DeletionFile>,

    /// RowIndex
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_id_meta: Option<RowIdMeta>,

    /// Original number of rows in the fragment. If this is None, then it is
    /// unknown. This is only optional for legacy reasons. All new tables should
    /// have this set.
    pub physical_rows: Option<usize>,

    /// Last updated at version metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_at_version_meta: Option<RowDatasetVersionMeta>,

    /// Created at version metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at_version_meta: Option<RowDatasetVersionMeta>,
}

impl Fragment {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            files: vec![],
            deletion_file: None,
            row_id_meta: None,
            physical_rows: None,
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        }
    }

    pub fn num_rows(&self) -> Option<usize> {
        match (self.physical_rows, &self.deletion_file) {
            // Known fragment length, no deletion file.
            (Some(len), None) => Some(len),
            // Known fragment length, but don't know deletion file size.
            (
                Some(len),
                Some(DeletionFile {
                    num_deleted_rows: Some(num_deleted_rows),
                    ..
                }),
            ) => Some(len - num_deleted_rows),
            _ => None,
        }
    }

    pub fn from_json(json: &str) -> Result<Self> {
        let fragment: Self = serde_json::from_str(json)?;
        Ok(fragment)
    }

    /// Create a `Fragment` with one DataFile
    pub fn with_file_legacy(
        id: u64,
        path: &str,
        schema: &Schema,
        physical_rows: Option<usize>,
    ) -> Self {
        Self {
            id,
            files: vec![DataFile::new_legacy(path, schema, None, None)],
            deletion_file: None,
            physical_rows,
            row_id_meta: None,
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        }
    }

    pub fn with_file(
        mut self,
        path: impl Into<String>,
        field_ids: Vec<i32>,
        column_indices: Vec<i32>,
        version: &LanceFileVersion,
        file_size_bytes: Option<NonZero<u64>>,
    ) -> Self {
        let (major, minor) = version.to_numbers();
        let data_file = DataFile::new(
            path,
            field_ids,
            column_indices,
            major,
            minor,
            file_size_bytes,
            None,
        );
        self.files.push(data_file);
        self
    }

    pub fn with_physical_rows(mut self, physical_rows: usize) -> Self {
        self.physical_rows = Some(physical_rows);
        self
    }

    pub fn add_file(
        &mut self,
        path: impl Into<String>,
        field_ids: Vec<i32>,
        column_indices: Vec<i32>,
        version: &LanceFileVersion,
        file_size_bytes: Option<NonZero<u64>>,
    ) {
        let (major, minor) = version.to_numbers();
        self.files.push(DataFile::new(
            path,
            field_ids,
            column_indices,
            major,
            minor,
            file_size_bytes,
            None,
        ));
    }

    /// Add a new [`DataFile`] to this fragment.
    pub fn add_file_legacy(&mut self, path: &str, schema: &Schema) {
        self.files
            .push(DataFile::new_legacy(path, schema, None, None));
    }

    // True if this fragment is made up of legacy v1 files, false otherwise
    pub fn has_legacy_files(&self) -> bool {
        // If any file in a fragment is legacy then all files in the fragment must be
        self.files[0].is_legacy_file()
    }

    // Helper method to infer the Lance version from a set of fragments
    //
    // Returns None if there are no data files
    // Returns an error if the data files have different versions
    pub fn try_infer_version(fragments: &[Self]) -> Result<Option<LanceFileVersion>> {
        // Otherwise we need to check the actual file versions
        // Determine version from first file
        let Some(sample_file) = fragments
            .iter()
            .find(|f| !f.files.is_empty())
            .map(|f| &f.files[0])
        else {
            return Ok(None);
        };
        let file_version = LanceFileVersion::try_from_major_minor(
            sample_file.file_major_version,
            sample_file.file_minor_version,
        )?;
        // Ensure all files match
        for frag in fragments {
            for file in &frag.files {
                let this_file_version = LanceFileVersion::try_from_major_minor(
                    file.file_major_version,
                    file.file_minor_version,
                )?;
                if file_version != this_file_version {
                    return Err(Error::invalid_input(
                        format!(
                            "All data files must have the same version.  Detected both {} and {}",
                            file_version, this_file_version
                        ),
                        location!(),
                    ));
                }
            }
        }
        Ok(Some(file_version))
    }
}

impl TryFrom<pb::DataFragment> for Fragment {
    type Error = Error;

    fn try_from(p: pb::DataFragment) -> Result<Self> {
        let physical_rows = if p.physical_rows > 0 {
            Some(p.physical_rows as usize)
        } else {
            None
        };
        Ok(Self {
            id: p.id,
            files: p
                .files
                .into_iter()
                .map(DataFile::try_from)
                .collect::<Result<_>>()?,
            deletion_file: p.deletion_file.map(DeletionFile::try_from).transpose()?,
            row_id_meta: p.row_id_sequence.map(RowIdMeta::try_from).transpose()?,
            physical_rows,
            last_updated_at_version_meta: p
                .last_updated_at_version_sequence
                .map(RowDatasetVersionMeta::try_from)
                .transpose()?,
            created_at_version_meta: p
                .created_at_version_sequence
                .map(RowDatasetVersionMeta::try_from)
                .transpose()?,
        })
    }
}

impl From<&Fragment> for pb::DataFragment {
    fn from(f: &Fragment) -> Self {
        let deletion_file = f.deletion_file.as_ref().map(|f| {
            let file_type = match f.file_type {
                DeletionFileType::Array => pb::deletion_file::DeletionFileType::ArrowArray,
                DeletionFileType::Bitmap => pb::deletion_file::DeletionFileType::Bitmap,
            };
            pb::DeletionFile {
                read_version: f.read_version,
                id: f.id,
                file_type: file_type.into(),
                num_deleted_rows: f.num_deleted_rows.unwrap_or_default() as u64,
                base_id: f.base_id,
            }
        });

        let row_id_sequence = f.row_id_meta.as_ref().map(|m| match m {
            RowIdMeta::Inline(data) => pb::data_fragment::RowIdSequence::InlineRowIds(data.clone()),
            RowIdMeta::External(file) => {
                pb::data_fragment::RowIdSequence::ExternalRowIds(pb::ExternalFile {
                    path: file.path.clone(),
                    offset: file.offset,
                    size: file.size,
                })
            }
        });
        let last_updated_at_version_sequence =
            last_updated_at_version_meta_to_pb(&f.last_updated_at_version_meta);
        let created_at_version_sequence = created_at_version_meta_to_pb(&f.created_at_version_meta);
        Self {
            id: f.id,
            files: f.files.iter().map(pb::DataFile::from).collect(),
            deletion_file,
            row_id_sequence,
            physical_rows: f.physical_rows.unwrap_or_default() as u64,
            last_updated_at_version_sequence,
            created_at_version_sequence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{
        DataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
    };
    use object_store::path::Path;
    use serde_json::{json, Value};

    #[test]
    fn test_new_fragment() {
        let path = "foobar.lance";

        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "s",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("si", DataType::Int32, false),
                    ArrowField::new("sb", DataType::Binary, true),
                ])),
                true,
            ),
            ArrowField::new("bool", DataType::Boolean, true),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        let fragment = Fragment::with_file_legacy(123, path, &schema, Some(10));

        assert_eq!(123, fragment.id);
        assert_eq!(
            fragment.files,
            vec![DataFile::new_legacy_from_fields(
                path.to_string(),
                vec![0, 1, 2, 3],
                None,
            )]
        )
    }

    #[test]
    fn test_roundtrip_fragment() {
        let mut fragment = Fragment::new(123);
        let schema = ArrowSchema::new(vec![ArrowField::new("x", DataType::Float16, true)]);
        fragment.add_file_legacy("foobar.lance", &Schema::try_from(&schema).unwrap());
        fragment.deletion_file = Some(DeletionFile {
            read_version: 123,
            id: 456,
            file_type: DeletionFileType::Array,
            num_deleted_rows: Some(10),
            base_id: None,
        });

        let proto = pb::DataFragment::from(&fragment);
        let fragment2 = Fragment::try_from(proto).unwrap();
        assert_eq!(fragment, fragment2);

        fragment.deletion_file = None;
        let proto = pb::DataFragment::from(&fragment);
        let fragment2 = Fragment::try_from(proto).unwrap();
        assert_eq!(fragment, fragment2);
    }

    #[test]
    fn test_to_json() {
        let mut fragment = Fragment::new(123);
        let schema = ArrowSchema::new(vec![ArrowField::new("x", DataType::Float16, true)]);
        fragment.add_file_legacy("foobar.lance", &Schema::try_from(&schema).unwrap());
        fragment.deletion_file = Some(DeletionFile {
            read_version: 123,
            id: 456,
            file_type: DeletionFileType::Array,
            num_deleted_rows: Some(10),
            base_id: None,
        });

        let json = serde_json::to_string(&fragment).unwrap();

        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            value,
            json!({
                "id": 123,
                "files":[
                    {"path": "foobar.lance", "fields": [0], "column_indices": [], 
                     "file_major_version": MAJOR_VERSION, "file_minor_version": MINOR_VERSION,
                     "file_size_bytes": null, "base_id": null }
                ],
                "deletion_file": {"read_version": 123, "id": 456, "file_type": "array",
                                  "num_deleted_rows": 10, "base_id": null},
                "physical_rows": None::<usize>}),
        );

        let frag2 = Fragment::from_json(&json).unwrap();
        assert_eq!(fragment, frag2);
    }

    #[test]
    fn data_file_validate_allows_extra_columns() {
        let data_file = DataFile {
            path: "foo.lance".to_string(),
            fields: vec![1, 2],
            // One extra column without a field id mapping
            column_indices: vec![0, 1, 2],
            file_major_version: MAJOR_VERSION as u32,
            file_minor_version: MINOR_VERSION as u32,
            file_size_bytes: Default::default(),
            base_id: None,
        };

        let base_path = Path::from("base");
        data_file
            .validate(&base_path)
            .expect("validation should allow extra columns without field ids");
    }
}
