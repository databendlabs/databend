// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Metadata columns (virtual/reserved fields) for Iceberg tables.
//!
//! This module defines metadata columns that can be requested in projections
//! but are not stored in data files. Instead, they are computed on-the-fly
//! during reading. Examples include the _file column (file path) and future
//! columns like partition values or row numbers.

use std::sync::Arc;

use once_cell::sync::Lazy;

use crate::spec::{NestedField, NestedFieldRef, PrimitiveType, Type};
use crate::{Error, ErrorKind, Result};

/// Reserved field ID for the file path (_file) column per Iceberg spec
pub const RESERVED_FIELD_ID_FILE: i32 = i32::MAX - 1;

/// Reserved field ID for the position (_pos) column per Iceberg spec
pub const RESERVED_FIELD_ID_POS: i32 = i32::MAX - 2;

/// Reserved field ID for the deleted (_deleted) column per Iceberg spec
pub const RESERVED_FIELD_ID_DELETED: i32 = i32::MAX - 3;

/// Reserved field ID for the spec ID (_spec_id) column per Iceberg spec
pub const RESERVED_FIELD_ID_SPEC_ID: i32 = i32::MAX - 4;

/// Reserved field ID for the partition (_partition) column per Iceberg spec
pub const RESERVED_FIELD_ID_PARTITION: i32 = i32::MAX - 5;

/// Reserved field ID for the file path in position delete files
pub const RESERVED_FIELD_ID_DELETE_FILE_PATH: i32 = i32::MAX - 101;

/// Reserved field ID for the position in position delete files
pub const RESERVED_FIELD_ID_DELETE_FILE_POS: i32 = i32::MAX - 102;

/// Reserved field ID for the change type (_change_type) column per Iceberg spec
pub const RESERVED_FIELD_ID_CHANGE_TYPE: i32 = i32::MAX - 104;

/// Reserved field ID for the change ordinal (_change_ordinal) column per Iceberg spec
pub const RESERVED_FIELD_ID_CHANGE_ORDINAL: i32 = i32::MAX - 105;

/// Reserved field ID for the commit snapshot ID (_commit_snapshot_id) column per Iceberg spec
pub const RESERVED_FIELD_ID_COMMIT_SNAPSHOT_ID: i32 = i32::MAX - 106;

/// Reserved field ID for the row ID (_row_id) column per Iceberg spec
pub const RESERVED_FIELD_ID_ROW_ID: i32 = i32::MAX - 107;

/// Reserved field ID for the last updated sequence number (_last_updated_sequence_number) column per Iceberg spec
pub const RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER: i32 = i32::MAX - 108;

/// Reserved column name for the file path metadata column
pub const RESERVED_COL_NAME_FILE: &str = "_file";

/// Reserved column name for the position metadata column
pub const RESERVED_COL_NAME_POS: &str = "_pos";

/// Reserved column name for the deleted metadata column
pub const RESERVED_COL_NAME_DELETED: &str = "_deleted";

/// Reserved column name for the spec ID metadata column
pub const RESERVED_COL_NAME_SPEC_ID: &str = "_spec_id";

/// Reserved column name for the partition metadata column
pub const RESERVED_COL_NAME_PARTITION: &str = "_partition";

/// Reserved column name for the file path in position delete files
pub const RESERVED_COL_NAME_DELETE_FILE_PATH: &str = "file_path";

/// Reserved column name for the position in position delete files
pub const RESERVED_COL_NAME_DELETE_FILE_POS: &str = "pos";

/// Reserved column name for the change type metadata column
pub const RESERVED_COL_NAME_CHANGE_TYPE: &str = "_change_type";

/// Reserved column name for the change ordinal metadata column
pub const RESERVED_COL_NAME_CHANGE_ORDINAL: &str = "_change_ordinal";

/// Reserved column name for the commit snapshot ID metadata column
pub const RESERVED_COL_NAME_COMMIT_SNAPSHOT_ID: &str = "_commit_snapshot_id";

/// Reserved column name for the row ID metadata column
pub const RESERVED_COL_NAME_ROW_ID: &str = "_row_id";

/// Reserved column name for the last updated sequence number metadata column
pub const RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER: &str = "_last_updated_sequence_number";

/// Lazy-initialized Iceberg field definition for the _file metadata column.
/// This field represents the file path as a required string field.
static FILE_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_FILE,
            RESERVED_COL_NAME_FILE,
            Type::Primitive(PrimitiveType::String),
        )
        .with_doc("Path of the file in which a row is stored"),
    )
});

/// Lazy-initialized Iceberg field definition for the _pos metadata column.
/// This field represents the ordinal position of a row in the source data file.
static POS_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_POS,
            RESERVED_COL_NAME_POS,
            Type::Primitive(PrimitiveType::Long),
        )
        .with_doc("Ordinal position of a row in the source data file"),
    )
});

/// Lazy-initialized Iceberg field definition for the _deleted metadata column.
/// This field indicates whether a row has been deleted.
static DELETED_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_DELETED,
            RESERVED_COL_NAME_DELETED,
            Type::Primitive(PrimitiveType::Boolean),
        )
        .with_doc("Whether the row has been deleted"),
    )
});

/// Lazy-initialized Iceberg field definition for the _spec_id metadata column.
/// This field represents the spec ID used to track the file containing a row.
static SPEC_ID_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_SPEC_ID,
            RESERVED_COL_NAME_SPEC_ID,
            Type::Primitive(PrimitiveType::Int),
        )
        .with_doc("Spec ID used to track the file containing a row"),
    )
});

/// Lazy-initialized Iceberg field definition for the file_path column in position delete files.
/// This field represents the path of a file in position-based delete files.
static DELETE_FILE_PATH_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_DELETE_FILE_PATH,
            RESERVED_COL_NAME_DELETE_FILE_PATH,
            Type::Primitive(PrimitiveType::String),
        )
        .with_doc("Path of a file, used in position-based delete files"),
    )
});

/// Lazy-initialized Iceberg field definition for the pos column in position delete files.
/// This field represents the ordinal position of a row in position-based delete files.
static DELETE_FILE_POS_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_DELETE_FILE_POS,
            RESERVED_COL_NAME_DELETE_FILE_POS,
            Type::Primitive(PrimitiveType::Long),
        )
        .with_doc("Ordinal position of a row, used in position-based delete files"),
    )
});

/// Lazy-initialized Iceberg field definition for the _change_type metadata column.
/// This field represents the record type in the changelog.
static CHANGE_TYPE_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_CHANGE_TYPE,
            RESERVED_COL_NAME_CHANGE_TYPE,
            Type::Primitive(PrimitiveType::String),
        )
        .with_doc(
            "The record type in the changelog (INSERT, DELETE, UPDATE_BEFORE, or UPDATE_AFTER)",
        ),
    )
});

/// Lazy-initialized Iceberg field definition for the _change_ordinal metadata column.
/// This field represents the order of the change.
static CHANGE_ORDINAL_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_CHANGE_ORDINAL,
            RESERVED_COL_NAME_CHANGE_ORDINAL,
            Type::Primitive(PrimitiveType::Int),
        )
        .with_doc("The order of the change"),
    )
});

/// Lazy-initialized Iceberg field definition for the _commit_snapshot_id metadata column.
/// This field represents the snapshot ID in which the change occurred.
static COMMIT_SNAPSHOT_ID_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_COMMIT_SNAPSHOT_ID,
            RESERVED_COL_NAME_COMMIT_SNAPSHOT_ID,
            Type::Primitive(PrimitiveType::Long),
        )
        .with_doc("The snapshot ID in which the change occurred"),
    )
});

/// Lazy-initialized Iceberg field definition for the _row_id metadata column.
/// This field represents a unique long assigned for row lineage.
static ROW_ID_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_ROW_ID,
            RESERVED_COL_NAME_ROW_ID,
            Type::Primitive(PrimitiveType::Long),
        )
        .with_doc("A unique long assigned for row lineage"),
    )
});

/// Lazy-initialized Iceberg field definition for the _last_updated_sequence_number metadata column.
/// This field represents the sequence number which last updated this row.
static LAST_UPDATED_SEQUENCE_NUMBER_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            Type::Primitive(PrimitiveType::Long),
        )
        .with_doc("The sequence number which last updated this row"),
    )
});

/// Returns the Iceberg field definition for the _file metadata column.
///
/// # Returns
/// A reference to the _file field definition as an Iceberg NestedField
pub fn file_field() -> &'static NestedFieldRef {
    &FILE_FIELD
}

/// Returns the Iceberg field definition for the _pos metadata column.
///
/// # Returns
/// A reference to the _pos field definition as an Iceberg NestedField
pub fn pos_field() -> &'static NestedFieldRef {
    &POS_FIELD
}

/// Returns the Iceberg field definition for the _deleted metadata column.
///
/// # Returns
/// A reference to the _deleted field definition as an Iceberg NestedField
pub fn deleted_field() -> &'static NestedFieldRef {
    &DELETED_FIELD
}

/// Returns the Iceberg field definition for the _spec_id metadata column.
///
/// # Returns
/// A reference to the _spec_id field definition as an Iceberg NestedField
pub fn spec_id_field() -> &'static NestedFieldRef {
    &SPEC_ID_FIELD
}

/// Returns the Iceberg field definition for the file_path column in position delete files.
///
/// # Returns
/// A reference to the file_path field definition as an Iceberg NestedField
pub fn delete_file_path_field() -> &'static NestedFieldRef {
    &DELETE_FILE_PATH_FIELD
}

/// Returns the Iceberg field definition for the pos column in position delete files.
///
/// # Returns
/// A reference to the pos field definition as an Iceberg NestedField
pub fn delete_file_pos_field() -> &'static NestedFieldRef {
    &DELETE_FILE_POS_FIELD
}

/// Returns the Iceberg field definition for the _change_type metadata column.
///
/// # Returns
/// A reference to the _change_type field definition as an Iceberg NestedField
pub fn change_type_field() -> &'static NestedFieldRef {
    &CHANGE_TYPE_FIELD
}

/// Returns the Iceberg field definition for the _change_ordinal metadata column.
///
/// # Returns
/// A reference to the _change_ordinal field definition as an Iceberg NestedField
pub fn change_ordinal_field() -> &'static NestedFieldRef {
    &CHANGE_ORDINAL_FIELD
}

/// Returns the Iceberg field definition for the _commit_snapshot_id metadata column.
///
/// # Returns
/// A reference to the _commit_snapshot_id field definition as an Iceberg NestedField
pub fn commit_snapshot_id_field() -> &'static NestedFieldRef {
    &COMMIT_SNAPSHOT_ID_FIELD
}

/// Returns the Iceberg field definition for the _row_id metadata column.
///
/// # Returns
/// A reference to the _row_id field definition as an Iceberg NestedField
pub fn row_id_field() -> &'static NestedFieldRef {
    &ROW_ID_FIELD
}

/// Returns the Iceberg field definition for the _last_updated_sequence_number metadata column.
///
/// # Returns
/// A reference to the _last_updated_sequence_number field definition as an Iceberg NestedField
pub fn last_updated_sequence_number_field() -> &'static NestedFieldRef {
    &LAST_UPDATED_SEQUENCE_NUMBER_FIELD
}

/// Creates the Iceberg field definition for the _partition metadata column.
///
/// The _partition field is a struct whose fields depend on the partition spec.
/// This function creates the field dynamically with the provided partition fields.
///
/// # Arguments
/// * `partition_fields` - The fields that make up the partition struct
///
/// # Returns
/// A new _partition field definition as an Iceberg NestedField
///
/// # Example
/// ```
/// use std::sync::Arc;
///
/// use iceberg::metadata_columns::partition_field;
/// use iceberg::spec::{NestedField, PrimitiveType, Type};
///
/// let fields = vec![
///     Arc::new(NestedField::required(
///         1,
///         "year",
///         Type::Primitive(PrimitiveType::Int),
///     )),
///     Arc::new(NestedField::required(
///         2,
///         "month",
///         Type::Primitive(PrimitiveType::Int),
///     )),
/// ];
/// let partition_field = partition_field(fields);
/// ```
pub fn partition_field(partition_fields: Vec<NestedFieldRef>) -> NestedFieldRef {
    use crate::spec::StructType;

    Arc::new(
        NestedField::required(
            RESERVED_FIELD_ID_PARTITION,
            RESERVED_COL_NAME_PARTITION,
            Type::Struct(StructType::new(partition_fields)),
        )
        .with_doc("Partition to which a row belongs"),
    )
}

/// Returns the Iceberg field definition for a metadata field ID.
///
/// Note: This function does not support `_partition` (field ID `i32::MAX - 5`) because
/// it's a struct field that requires dynamic partition fields. Use `partition_field()`
/// instead to create the `_partition` field with the appropriate partition fields.
///
/// # Arguments
/// * `field_id` - The metadata field ID
///
/// # Returns
/// The Iceberg field definition for the metadata column, or an error if not a metadata field
pub fn get_metadata_field(field_id: i32) -> Result<&'static NestedFieldRef> {
    match field_id {
        RESERVED_FIELD_ID_FILE => Ok(file_field()),
        RESERVED_FIELD_ID_POS => Ok(pos_field()),
        RESERVED_FIELD_ID_DELETED => Ok(deleted_field()),
        RESERVED_FIELD_ID_SPEC_ID => Ok(spec_id_field()),
        RESERVED_FIELD_ID_PARTITION => Err(Error::new(
            ErrorKind::Unexpected,
            "The _partition field must be created using partition_field() with appropriate partition fields",
        )),
        RESERVED_FIELD_ID_DELETE_FILE_PATH => Ok(delete_file_path_field()),
        RESERVED_FIELD_ID_DELETE_FILE_POS => Ok(delete_file_pos_field()),
        RESERVED_FIELD_ID_CHANGE_TYPE => Ok(change_type_field()),
        RESERVED_FIELD_ID_CHANGE_ORDINAL => Ok(change_ordinal_field()),
        RESERVED_FIELD_ID_COMMIT_SNAPSHOT_ID => Ok(commit_snapshot_id_field()),
        RESERVED_FIELD_ID_ROW_ID => Ok(row_id_field()),
        RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER => Ok(last_updated_sequence_number_field()),
        _ if is_metadata_field(field_id) => {
            // Future metadata fields can be added here
            Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Metadata field ID {field_id} recognized but field definition not implemented"
                ),
            ))
        }
        _ => Err(Error::new(
            ErrorKind::Unexpected,
            format!("Field ID {field_id} is not a metadata field"),
        )),
    }
}

/// Returns the field ID for a metadata column name.
///
/// # Arguments
/// * `column_name` - The metadata column name
///
/// # Returns
/// The field ID of the metadata column, or an error if the column name is not recognized
pub fn get_metadata_field_id(column_name: &str) -> Result<i32> {
    match column_name {
        RESERVED_COL_NAME_FILE => Ok(RESERVED_FIELD_ID_FILE),
        RESERVED_COL_NAME_POS => Ok(RESERVED_FIELD_ID_POS),
        RESERVED_COL_NAME_DELETED => Ok(RESERVED_FIELD_ID_DELETED),
        RESERVED_COL_NAME_SPEC_ID => Ok(RESERVED_FIELD_ID_SPEC_ID),
        RESERVED_COL_NAME_PARTITION => Ok(RESERVED_FIELD_ID_PARTITION),
        RESERVED_COL_NAME_DELETE_FILE_PATH => Ok(RESERVED_FIELD_ID_DELETE_FILE_PATH),
        RESERVED_COL_NAME_DELETE_FILE_POS => Ok(RESERVED_FIELD_ID_DELETE_FILE_POS),
        RESERVED_COL_NAME_CHANGE_TYPE => Ok(RESERVED_FIELD_ID_CHANGE_TYPE),
        RESERVED_COL_NAME_CHANGE_ORDINAL => Ok(RESERVED_FIELD_ID_CHANGE_ORDINAL),
        RESERVED_COL_NAME_COMMIT_SNAPSHOT_ID => Ok(RESERVED_FIELD_ID_COMMIT_SNAPSHOT_ID),
        RESERVED_COL_NAME_ROW_ID => Ok(RESERVED_FIELD_ID_ROW_ID),
        RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER => {
            Ok(RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
        }
        _ => Err(Error::new(
            ErrorKind::Unexpected,
            format!("Unknown/unsupported metadata column name: {column_name}"),
        )),
    }
}

/// Checks if a field ID is a metadata field.
///
/// # Arguments
/// * `field_id` - The field ID to check
///
/// # Returns
/// `true` if the field ID is a (currently supported) metadata field, `false` otherwise
pub fn is_metadata_field(field_id: i32) -> bool {
    matches!(
        field_id,
        RESERVED_FIELD_ID_FILE
            | RESERVED_FIELD_ID_POS
            | RESERVED_FIELD_ID_DELETED
            | RESERVED_FIELD_ID_SPEC_ID
            | RESERVED_FIELD_ID_PARTITION
            | RESERVED_FIELD_ID_DELETE_FILE_PATH
            | RESERVED_FIELD_ID_DELETE_FILE_POS
            | RESERVED_FIELD_ID_CHANGE_TYPE
            | RESERVED_FIELD_ID_CHANGE_ORDINAL
            | RESERVED_FIELD_ID_COMMIT_SNAPSHOT_ID
            | RESERVED_FIELD_ID_ROW_ID
            | RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
    )
}

/// Checks if a column name is a metadata column.
///
/// # Arguments
/// * `column_name` - The column name to check
///
/// # Returns
/// `true` if the column name is a metadata column, `false` otherwise
pub fn is_metadata_column_name(column_name: &str) -> bool {
    get_metadata_field_id(column_name).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::PrimitiveType;

    #[test]
    fn test_partition_field_creation() {
        // Create partition fields for a hypothetical year/month partition
        let partition_fields = vec![
            Arc::new(NestedField::required(
                1000,
                "year",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::required(
                1001,
                "month",
                Type::Primitive(PrimitiveType::Int),
            )),
        ];

        // Create the _partition metadata field
        let partition = partition_field(partition_fields);

        // Verify field properties
        assert_eq!(partition.id, RESERVED_FIELD_ID_PARTITION);
        assert_eq!(partition.name, RESERVED_COL_NAME_PARTITION);
        assert!(partition.required);

        // Verify it's a struct type with correct fields
        if let Type::Struct(struct_type) = partition.field_type.as_ref() {
            assert_eq!(struct_type.fields().len(), 2);
            assert_eq!(struct_type.fields()[0].name, "year");
            assert_eq!(struct_type.fields()[1].name, "month");
        } else {
            panic!("Expected struct type for _partition field");
        }
    }

    #[test]
    fn test_partition_field_id_recognized() {
        assert!(is_metadata_field(RESERVED_FIELD_ID_PARTITION));
    }

    #[test]
    fn test_partition_field_name_recognized() {
        assert_eq!(
            get_metadata_field_id(RESERVED_COL_NAME_PARTITION).unwrap(),
            RESERVED_FIELD_ID_PARTITION
        );
    }

    #[test]
    fn test_get_metadata_field_returns_error_for_partition() {
        // partition field requires dynamic creation, so get_metadata_field should return an error
        let result = get_metadata_field(RESERVED_FIELD_ID_PARTITION);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("partition_field()")
        );
    }

    #[test]
    fn test_all_metadata_field_ids() {
        // Test that all non-partition metadata fields can be retrieved
        assert!(get_metadata_field(RESERVED_FIELD_ID_FILE).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_POS).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_DELETED).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_SPEC_ID).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_DELETE_FILE_PATH).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_DELETE_FILE_POS).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_CHANGE_TYPE).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_CHANGE_ORDINAL).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_COMMIT_SNAPSHOT_ID).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_ROW_ID).is_ok());
        assert!(get_metadata_field(RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER).is_ok());
    }
}
