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

use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use databend_common_exception::ErrorCode;

/// Fuse engine table type.
/// This is used to distinguish different table types.
#[derive(Clone, PartialEq)]
pub enum FuseTableType {
    // Standard table with full functionality.
    Standard,
    // External table, external location.
    External,
    // Table attached to the system.
    Attached,
}

impl FuseTableType {
    pub fn is_readonly(&self) -> bool {
        // Match all types avoid missing one if someone adds a new type.
        match self {
            FuseTableType::Standard => false,
            FuseTableType::External => false,
            FuseTableType::Attached => true,
        }
    }
}

/// Fuse engine table format.
/// This is used to distinguish different table formats.
#[derive(Clone, Copy, Debug)]
pub enum FuseStorageFormat {
    Parquet,
    /// Tombstone for storage formats that are no longer supported (e.g. the
    /// removed `native` format). Tables using such a format can still be
    /// instantiated so that metadata-only operations (SHOW, DESC, DROP) keep
    /// working, but any operation touching the data must be rejected via
    /// [`crate::FuseTable::check_format_supported`].
    Unsupported,
}

impl Display for FuseStorageFormat {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            FuseStorageFormat::Parquet => write!(f, "Parquet"),
            FuseStorageFormat::Unsupported => write!(f, "Unsupported"),
        }
    }
}

impl FromStr for FuseStorageFormat {
    type Err = ErrorCode;

    /// Strict parsing used when validating CREATE / ALTER ... SET OPTIONS.
    /// The removed `native` format and any unknown format are rejected so that
    /// new tables can never be created with an unsupported storage format.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "" | "parquet" => Ok(FuseStorageFormat::Parquet),
            "native" => Err(ErrorCode::UnknownFormat(
                "native storage_format is no longer supported".to_string(),
            )),
            other => Err(ErrorCode::UnknownFormat(format!(
                "unknown fuse storage_format {}",
                other
            ))),
        }
    }
}

impl FuseStorageFormat {
    /// Lenient parsing used when instantiating an existing table from its
    /// stored options. Unknown / removed formats become
    /// [`FuseStorageFormat::Unsupported`] so that instantiation never fails;
    /// this keeps catalog-wide operations (e.g. listing tables) working even
    /// when some tables use a format that is no longer supported. The original
    /// format string remains available via the table options for diagnostics.
    pub fn from_table_option(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "" | "parquet" => FuseStorageFormat::Parquet,
            _ => FuseStorageFormat::Unsupported,
        }
    }
}

/// Defensive error for code paths that operate on block data but receive a
/// table whose storage format is no longer supported. These paths are
/// unreachable in practice because [`crate::FuseTable::check_format_supported`]
/// rejects such tables at the operation entry point; this helper exists so the
/// low-level `match` arms can stay exhaustive without panicking.
pub fn unsupported_storage_format_error() -> ErrorCode {
    ErrorCode::StorageUnsupported(
        "storage_format is no longer supported for read/write; the table can only be dropped",
    )
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FuseSegmentFormat {
    Row,
    Column,
}

pub fn segment_format_from_location(location: &str) -> FuseSegmentFormat {
    if location.ends_with("col") {
        FuseSegmentFormat::Column
    } else {
        FuseSegmentFormat::Row
    }
}

impl FromStr for FuseSegmentFormat {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "" | "row_oriented" => Ok(FuseSegmentFormat::Row),
            "column_oriented" => Ok(FuseSegmentFormat::Column),
            other => Err(ErrorCode::UnknownFormat(format!(
                "unknown fuse segment_format {}",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CREATE / ALTER ... SET OPTIONS path: strict parsing must reject the
    /// removed `native` format and any unknown format, so new tables can never
    /// be created with an unsupported storage format.
    #[test]
    fn strict_from_str_rejects_native_and_unknown() {
        assert!(matches!(
            FuseStorageFormat::from_str(""),
            Ok(FuseStorageFormat::Parquet)
        ));
        assert!(matches!(
            FuseStorageFormat::from_str("parquet"),
            Ok(FuseStorageFormat::Parquet)
        ));
        assert!(matches!(
            FuseStorageFormat::from_str("PARQUET"),
            Ok(FuseStorageFormat::Parquet)
        ));
        assert!(FuseStorageFormat::from_str("native").is_err());
        assert!(FuseStorageFormat::from_str("bogus").is_err());
    }

    /// Instantiation path (FuseTable::try_create): lenient parsing must never
    /// fail, mapping removed/unknown formats to the `Unsupported` tombstone so
    /// that listing and dropping tables keeps working catalog-wide.
    #[test]
    fn lenient_from_table_option_tolerates_native() {
        assert!(matches!(
            FuseStorageFormat::from_table_option(""),
            FuseStorageFormat::Parquet
        ));
        assert!(matches!(
            FuseStorageFormat::from_table_option("parquet"),
            FuseStorageFormat::Parquet
        ));
        assert!(matches!(
            FuseStorageFormat::from_table_option("native"),
            FuseStorageFormat::Unsupported
        ));
        assert!(matches!(
            FuseStorageFormat::from_table_option("some_future_format"),
            FuseStorageFormat::Unsupported
        ));
    }
}
