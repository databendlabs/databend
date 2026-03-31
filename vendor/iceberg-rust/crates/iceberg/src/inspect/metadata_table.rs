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

use super::{ManifestsTable, SnapshotsTable};
use crate::table::Table;

/// Metadata table is used to inspect a table's history, snapshots, and other metadata as a table.
///
/// References:
/// - <https://github.com/apache/iceberg/blob/ac865e334e143dfd9e33011d8cf710b46d91f1e5/core/src/main/java/org/apache/iceberg/MetadataTableType.java#L23-L39>
/// - <https://iceberg.apache.org/docs/latest/spark-queries/#querying-with-sql>
/// - <https://py.iceberg.apache.org/api/#inspecting-tables>
#[derive(Debug)]
pub struct MetadataTable<'a>(&'a Table);

/// Metadata table type.
#[derive(Debug, Clone, strum::EnumIter)]
pub enum MetadataTableType {
    /// [`SnapshotsTable`]
    Snapshots,
    /// [`ManifestsTable`]
    Manifests,
}

impl MetadataTableType {
    /// Returns the string representation of the metadata table type.
    pub fn as_str(&self) -> &str {
        match self {
            MetadataTableType::Snapshots => "snapshots",
            MetadataTableType::Manifests => "manifests",
        }
    }

    /// Returns all the metadata table types.
    pub fn all_types() -> impl Iterator<Item = Self> {
        use strum::IntoEnumIterator;
        Self::iter()
    }
}

impl TryFrom<&str> for MetadataTableType {
    type Error = String;

    fn try_from(value: &str) -> std::result::Result<Self, String> {
        match value {
            "snapshots" => Ok(Self::Snapshots),
            "manifests" => Ok(Self::Manifests),
            _ => Err(format!("invalid metadata table type: {value}")),
        }
    }
}

impl<'a> MetadataTable<'a> {
    /// Creates a new metadata scan.
    pub fn new(table: &'a Table) -> Self {
        Self(table)
    }

    /// Get the snapshots table.
    pub fn snapshots(&self) -> SnapshotsTable<'_> {
        SnapshotsTable::new(self.0)
    }

    /// Get the manifests table.
    pub fn manifests(&self) -> ManifestsTable<'_> {
        ManifestsTable::new(self.0)
    }
}
