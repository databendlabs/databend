use serde::Deserialize;
use serde::Serialize;

use crate::schema::ColumnId;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableFieldIndex(usize);

impl TableFieldIndex {
    #[inline]
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DataFieldIndex(usize);

impl DataFieldIndex {
    #[inline]
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0
    }
}

/// The position of a leaf column in a schema view.
///
/// Note: This is an ordinal/position, not a stable identifier.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LeafColumnIndex(usize);

impl LeafColumnIndex {
    #[inline]
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0
    }
}

/// Field id stored in parquet/arrow schema metadata (e.g. "PARQUET:field_id").
///
/// Note: This is an external identifier. It may (by convention) carry Databend's ColumnId,
/// but this mapping must be explicit at call sites.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ParquetFieldId(u32);

impl ParquetFieldId {
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

#[inline]
pub const fn column_id_from_parquet_field_id(id: ParquetFieldId) -> ColumnId {
    id.0
}

#[inline]
pub const fn parquet_field_id_from_column_id(id: ColumnId) -> ParquetFieldId {
    ParquetFieldId(id)
}
