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
    // Shared table with read-only access.
    SharedReadOnly,
}

impl FuseTableType {
    pub fn is_readonly(&self) -> bool {
        // Match all types avoid missing one if someone adds a new type.
        match self {
            FuseTableType::Standard => false,
            FuseTableType::External => false,
            FuseTableType::Attached => true,
            FuseTableType::SharedReadOnly => true,
        }
    }
}

/// Fuse engine table format.
/// This is used to distinguish different table formats.
#[derive(Clone, Copy, Debug)]
pub enum FuseStorageFormat {
    Parquet,
    Native,
}

impl FromStr for FuseStorageFormat {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "" | "parquet" => Ok(FuseStorageFormat::Parquet),
            "native" => Ok(FuseStorageFormat::Native),
            other => Err(ErrorCode::UnknownFormat(format!(
                "unknown fuse storage_format {}",
                other
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FuseSegmentFormat {
    Row,
    Column,
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
