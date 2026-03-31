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

//! Table API for Apache Iceberg

use std::sync::Arc;

use crate::arrow::ArrowReaderBuilder;
use crate::inspect::MetadataTable;
use crate::io::FileIO;
use crate::io::object_cache::ObjectCache;
use crate::scan::TableScanBuilder;
use crate::spec::{SchemaRef, TableMetadata, TableMetadataRef};
use crate::{Error, ErrorKind, Result, TableIdent};

/// Builder to create table scan.
pub struct TableBuilder {
    file_io: Option<FileIO>,
    metadata_location: Option<String>,
    metadata: Option<TableMetadataRef>,
    identifier: Option<TableIdent>,
    readonly: bool,
    disable_cache: bool,
    cache_size_bytes: Option<u64>,
}

impl TableBuilder {
    pub(crate) fn new() -> Self {
        Self {
            file_io: None,
            metadata_location: None,
            metadata: None,
            identifier: None,
            readonly: false,
            disable_cache: false,
            cache_size_bytes: None,
        }
    }

    /// required - sets the necessary FileIO to use for the table
    pub fn file_io(mut self, file_io: FileIO) -> Self {
        self.file_io = Some(file_io);
        self
    }

    /// optional - sets the tables metadata location
    pub fn metadata_location<T: Into<String>>(mut self, metadata_location: T) -> Self {
        self.metadata_location = Some(metadata_location.into());
        self
    }

    /// required - passes in the TableMetadata to use for the Table
    pub fn metadata<T: Into<TableMetadataRef>>(mut self, metadata: T) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// required - passes in the TableIdent to use for the Table
    pub fn identifier(mut self, identifier: TableIdent) -> Self {
        self.identifier = Some(identifier);
        self
    }

    /// specifies if the Table is readonly or not (default not)
    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// specifies if the Table's metadata cache will be disabled,
    /// so that reads of Manifests and ManifestLists will never
    /// get cached.
    pub fn disable_cache(mut self) -> Self {
        self.disable_cache = true;
        self
    }

    /// optionally set a non-default metadata cache size
    pub fn cache_size_bytes(mut self, cache_size_bytes: u64) -> Self {
        self.cache_size_bytes = Some(cache_size_bytes);
        self
    }

    /// build the Table
    pub fn build(self) -> Result<Table> {
        let Self {
            file_io,
            metadata_location,
            metadata,
            identifier,
            readonly,
            disable_cache,
            cache_size_bytes,
        } = self;

        let Some(file_io) = file_io else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "FileIO must be provided with TableBuilder.file_io()",
            ));
        };

        let Some(metadata) = metadata else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "TableMetadataRef must be provided with TableBuilder.metadata()",
            ));
        };

        let Some(identifier) = identifier else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "TableIdent must be provided with TableBuilder.identifier()",
            ));
        };

        let object_cache = if disable_cache {
            Arc::new(ObjectCache::with_disabled_cache(file_io.clone()))
        } else if let Some(cache_size_bytes) = cache_size_bytes {
            Arc::new(ObjectCache::new_with_capacity(
                file_io.clone(),
                cache_size_bytes,
            ))
        } else {
            Arc::new(ObjectCache::new(file_io.clone()))
        };

        Ok(Table {
            file_io,
            metadata_location,
            metadata,
            identifier,
            readonly,
            object_cache,
        })
    }
}

/// Table represents a table in the catalog.
#[derive(Debug, Clone)]
pub struct Table {
    file_io: FileIO,
    metadata_location: Option<String>,
    metadata: TableMetadataRef,
    identifier: TableIdent,
    readonly: bool,
    object_cache: Arc<ObjectCache>,
}

impl Table {
    /// Sets the [`Table`] metadata and returns an updated instance with the new metadata applied.
    pub(crate) fn with_metadata(mut self, metadata: TableMetadataRef) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets the [`Table`] metadata location and returns an updated instance.
    pub(crate) fn with_metadata_location(mut self, metadata_location: String) -> Self {
        self.metadata_location = Some(metadata_location);
        self
    }

    /// Returns a TableBuilder to build a table
    pub fn builder() -> TableBuilder {
        TableBuilder::new()
    }

    /// Returns table identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }
    /// Returns current metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Returns current metadata ref.
    pub fn metadata_ref(&self) -> TableMetadataRef {
        self.metadata.clone()
    }

    /// Returns current metadata location.
    pub fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    /// Returns current metadata location in a result.
    pub fn metadata_location_result(&self) -> Result<&str> {
        self.metadata_location.as_deref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Metadata location does not exist for table: {}",
                self.identifier
            ),
        ))
    }

    /// Returns file io used in this table.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Returns this table's object cache
    pub(crate) fn object_cache(&self) -> Arc<ObjectCache> {
        self.object_cache.clone()
    }

    /// Creates a table scan.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        TableScanBuilder::new(self)
    }

    /// Creates a metadata table which provides table-like APIs for inspecting metadata.
    /// See [`MetadataTable`] for more details.
    pub fn inspect(&self) -> MetadataTable<'_> {
        MetadataTable::new(self)
    }

    /// Returns the flag indicating whether the `Table` is readonly or not
    pub fn readonly(&self) -> bool {
        self.readonly
    }

    /// Returns the current schema as a shared reference.
    pub fn current_schema_ref(&self) -> SchemaRef {
        self.metadata.current_schema().clone()
    }

    /// Create a reader for the table.
    pub fn reader_builder(&self) -> ArrowReaderBuilder {
        ArrowReaderBuilder::new(self.file_io.clone())
    }
}

/// `StaticTable` is a read-only table struct that can be created from a metadata file or from `TableMetaData` without a catalog.
/// It can only be used to read metadata and for table scan.
/// # Examples
///
/// ```rust, no_run
/// # use iceberg::io::FileIO;
/// # use iceberg::table::StaticTable;
/// # use iceberg::TableIdent;
/// # async fn example() {
/// let metadata_file_location = "s3://bucket_name/path/to/metadata.json";
/// let file_io = FileIO::from_path(&metadata_file_location)
///     .unwrap()
///     .build()
///     .unwrap();
/// let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
/// let static_table =
///     StaticTable::from_metadata_file(&metadata_file_location, static_identifier, file_io)
///         .await
///         .unwrap();
/// let snapshot_id = static_table
///     .metadata()
///     .current_snapshot()
///     .unwrap()
///     .snapshot_id();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StaticTable(Table);

impl StaticTable {
    /// Creates a static table from a given `TableMetadata` and `FileIO`
    pub async fn from_metadata(
        metadata: TableMetadata,
        table_ident: TableIdent,
        file_io: FileIO,
    ) -> Result<Self> {
        let table = Table::builder()
            .metadata(metadata)
            .identifier(table_ident)
            .file_io(file_io.clone())
            .readonly(true)
            .build();

        Ok(Self(table?))
    }
    /// Creates a static table directly from metadata file and `FileIO`
    pub async fn from_metadata_file(
        metadata_location: &str,
        table_ident: TableIdent,
        file_io: FileIO,
    ) -> Result<Self> {
        let metadata = TableMetadata::read_from(&file_io, metadata_location).await?;

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location(metadata_location)
            .identifier(table_ident)
            .file_io(file_io.clone())
            .readonly(true)
            .build();

        Ok(Self(table?))
    }

    /// Create a TableScanBuilder for the static table.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        self.0.scan()
    }

    /// Get TableMetadataRef for the static table
    pub fn metadata(&self) -> TableMetadataRef {
        self.0.metadata_ref()
    }

    /// Consumes the `StaticTable` and return it as a `Table`
    /// Please use this method carefully as the Table it returns remains detached from a catalog
    /// and can't be used to perform modifications on the table.
    pub fn into_table(self) -> Table {
        self.0
    }

    /// Create a reader for the table.
    pub fn reader_builder(&self) -> ArrowReaderBuilder {
        ArrowReaderBuilder::new(self.0.file_io.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_static_table_from_file() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        let snapshot_id = static_table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        assert_eq!(
            snapshot_id, 3055729675574597004,
            "snapshot id from metadata don't match"
        );
    }

    #[tokio::test]
    async fn test_static_into_table() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        let table = static_table.into_table();
        assert!(table.readonly());
        assert_eq!(table.identifier.name(), "static_table");
        assert_eq!(
            table.metadata_location(),
            Some(metadata_file_path).as_deref()
        );
    }

    #[tokio::test]
    async fn test_table_readonly_flag() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let metadata_file = file_io.new_input(metadata_file_path).unwrap();
        let metadata_file_content = metadata_file.read().await.unwrap();
        let table_metadata =
            serde_json::from_slice::<TableMetadata>(&metadata_file_content).unwrap();
        let static_identifier = TableIdent::from_strs(["ns", "table"]).unwrap();
        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(static_identifier)
            .file_io(file_io.clone())
            .build()
            .unwrap();
        assert!(!table.readonly());
        assert_eq!(table.identifier.name(), "table");
    }
}
