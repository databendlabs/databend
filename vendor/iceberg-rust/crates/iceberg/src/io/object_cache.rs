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

use std::mem::size_of_val;
use std::sync::Arc;

use crate::io::FileIO;
use crate::spec::{
    FormatVersion, Manifest, ManifestFile, ManifestList, SchemaId, SnapshotRef, TableMetadataRef,
};
use crate::{Error, ErrorKind, Result};

const DEFAULT_CACHE_SIZE_BYTES: u64 = 32 * 1024 * 1024; // 32MB

#[derive(Clone, Debug)]
pub(crate) enum CachedItem {
    ManifestList(Arc<ManifestList>),
    Manifest(Arc<Manifest>),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum CachedObjectKey {
    ManifestList((String, FormatVersion, SchemaId)),
    Manifest(String),
}

/// Caches metadata objects deserialized from immutable files
#[derive(Clone, Debug)]
pub struct ObjectCache {
    cache: moka::future::Cache<CachedObjectKey, CachedItem>,
    file_io: FileIO,
    cache_disabled: bool,
}

impl ObjectCache {
    /// Creates a new [`ObjectCache`]
    /// with the default cache size
    pub(crate) fn new(file_io: FileIO) -> Self {
        Self::new_with_capacity(file_io, DEFAULT_CACHE_SIZE_BYTES)
    }

    /// Creates a new [`ObjectCache`]
    /// with a specific cache size
    pub(crate) fn new_with_capacity(file_io: FileIO, cache_size_bytes: u64) -> Self {
        if cache_size_bytes == 0 {
            Self::with_disabled_cache(file_io)
        } else {
            Self {
                cache: moka::future::Cache::builder()
                    .weigher(|_, val: &CachedItem| match val {
                        CachedItem::ManifestList(item) => size_of_val(item.as_ref()),
                        CachedItem::Manifest(item) => size_of_val(item.as_ref()),
                    } as u32)
                    .max_capacity(cache_size_bytes)
                    .build(),
                file_io,
                cache_disabled: false,
            }
        }
    }

    /// Creates a new [`ObjectCache`]
    /// with caching disabled
    pub(crate) fn with_disabled_cache(file_io: FileIO) -> Self {
        Self {
            cache: moka::future::Cache::new(0),
            file_io,
            cache_disabled: true,
        }
    }

    /// Retrieves an Arc [`Manifest`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub(crate) async fn get_manifest(&self, manifest_file: &ManifestFile) -> Result<Arc<Manifest>> {
        if self.cache_disabled {
            return manifest_file
                .load_manifest(&self.file_io)
                .await
                .map(Arc::new);
        }

        let key = CachedObjectKey::Manifest(manifest_file.manifest_path.clone());

        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest(manifest_file))
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to load manifest {}", manifest_file.manifest_path),
                )
                .with_source(err)
            })?
            .into_value();

        match cache_entry {
            CachedItem::Manifest(arc_manifest) => Ok(arc_manifest),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for key '{key:?}' is not a Manifest"),
            )),
        }
    }

    /// Retrieves an Arc [`ManifestList`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub(crate) async fn get_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<Arc<ManifestList>> {
        if self.cache_disabled {
            return snapshot
                .load_manifest_list(&self.file_io, table_metadata)
                .await
                .map(Arc::new);
        }

        let key = CachedObjectKey::ManifestList((
            snapshot.manifest_list().to_string(),
            table_metadata.format_version,
            snapshot.schema_id().unwrap(),
        ));
        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest_list(snapshot, table_metadata))
            .await
            .map_err(|err| {
                Arc::try_unwrap(err).unwrap_or_else(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to load manifest list in cache",
                    )
                    .with_source(err)
                })
            })?
            .into_value();

        match cache_entry {
            CachedItem::ManifestList(arc_manifest_list) => Ok(arc_manifest_list),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for path '{key:?}' is not a manifest list"),
            )),
        }
    }

    async fn fetch_and_parse_manifest(&self, manifest_file: &ManifestFile) -> Result<CachedItem> {
        let manifest = manifest_file.load_manifest(&self.file_io).await?;

        Ok(CachedItem::Manifest(Arc::new(manifest)))
    }

    async fn fetch_and_parse_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<CachedItem> {
        let manifest_list = snapshot
            .load_manifest_list(&self.file_io, table_metadata)
            .await?;

        Ok(CachedItem::ManifestList(Arc::new(manifest_list)))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use minijinja::value::Value;
    use minijinja::{AutoEscape, Environment, context};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::TableIdent;
    use crate::io::{FileIO, OutputFile};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Struct, TableMetadata,
    };
    use crate::table::Table;

    fn render_template(template: &str, ctx: Value) -> String {
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_| AutoEscape::None);
        env.render_str(template, ctx).unwrap()
    }

    struct TableTestFixture {
        table_location: String,
        table: Table,
    }

    impl TableTestFixture {
        fn new() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
                .unwrap()
                .build()
                .unwrap();

            let table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let metadata_json = render_template(&template_json_str, context! {
                    table_location => &table_location,
                    manifest_list_1_location => &manifest_list1_location,
                    manifest_list_2_location => &manifest_list2_location,
                    table_metadata_1_location => &table_metadata1_location,
                });
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
                .build()
                .unwrap();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        fn next_manifest_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    self.table_location,
                    Uuid::new_v4()
                ))
                .unwrap()
        }

        async fn setup_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = self.table.metadata().default_partition_spec();

            // Write data files
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                None,
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_data();
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/1.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(100))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            let data_file_manifest = writer.write_manifest_file().await.unwrap();

            // Write to manifest list
            let mut manifest_list_write = ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(current_snapshot.manifest_list())
                    .unwrap(),
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_disabled_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::with_disabled_cache(fixture.table.file_io().clone());

        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        let manifest_file = result_manifest_list.entries().first().unwrap();
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_default_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::new(fixture.table.file_io().clone());

        // not in cache
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        // retrieve cached version
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        let manifest_file = result_manifest_list.entries().first().unwrap();

        // not in cache
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );

        // retrieve cached version
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );
    }
}
