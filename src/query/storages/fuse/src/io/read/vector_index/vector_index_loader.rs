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

//! Loads vector index data from either file format: legacy combined Parquet files
//! (below) and per-index bundles (`vector_index_bundle_loader`). The format follows
//! from the block metadata (`IndexFileLocation`), so callers never branch on it;
//! bundle identity and row count are checked here, once, for every read.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::types::DataType;
use databend_common_metrics::storage::metrics_inc_block_vector_index_read_bytes;
use databend_common_metrics::storage::metrics_inc_block_vector_index_read_milliseconds;
use databend_storages_common_cache::MemCache;
use databend_storages_common_index::VectorBundleEntry;
use databend_storages_common_index::VectorBundleFooter;
use databend_storages_common_index::VectorBundlePageRead;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_index::vector_fp16_data_name;
use databend_storages_common_index::vector_validity_name;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::IndexFileLocation;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::table::VECTOR_REFINE_PAGE_ROWS;
use opendal::Operator;

use super::vector_index_bundle_loader::load_bundle_components;
use super::vector_index_bundle_loader::load_bundle_footer;
use super::vector_index_bundle_loader::load_bundle_reads;
use crate::index::VectorIndexFile;
use crate::io::FuseCaches;
use crate::io::IndexMetaCache;
use crate::io::read::index_loader::IndexLoadSpec;
use crate::io::read::index_loader::load_index_files;
use crate::io::read::index_loader::load_index_meta;

struct VectorIndexLoadSpec;

impl IndexLoadSpec for VectorIndexLoadSpec {
    type Meta = VectorIndexMeta;
    type File = VectorIndexFile;

    fn meta_cache(caches: &IndexMetaCache) -> &MemCache<Arc<Self::Meta>> {
        &caches.vector
    }

    // Raw HNSW component bytes are not cached: the decoded index is kept in
    // `FuseCaches::vector_indexes`, so caching the bytes as well would store it twice.
    fn file_cache(_caches: &FuseCaches) -> Option<&MemCache<Self::File>> {
        None
    }

    fn meta_columns(meta: &Self::Meta) -> &Vec<(String, SingleColumnMeta)> {
        static NO_COLUMNS: Vec<(String, SingleColumnMeta)> = Vec::new();
        match meta {
            VectorIndexMeta::Legacy(meta) => &meta.columns,
            // Not reachable: bundle files are read by the bundle loader.
            VectorIndexMeta::Bundle(_) => &NO_COLUMNS,
        }
    }

    fn make_file(name: String, data: Bytes) -> Self::File {
        VectorIndexFile::create(name, data)
    }

    fn file_data(file: &Self::File) -> &Bytes {
        &file.data
    }

    fn arrow_data_type(_column_name: &str) -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Binary
    }

    fn column_data_type(_column_name: &str) -> DataType {
        DataType::Binary
    }

    fn record_metrics(bytes: u64, ms: u64) {
        metrics_inc_block_vector_index_read_bytes(bytes);
        metrics_inc_block_vector_index_read_milliseconds(ms);
    }
}

/// Loads vector index meta data
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
pub async fn load_vector_index_meta(dal: Operator, path: &str) -> Result<Arc<VectorIndexMeta>> {
    load_index_meta::<VectorIndexLoadSpec>(dal, path).await
}

/// The vector index a reader expects, checked against the file it reads.
#[derive(Clone, Debug)]
pub struct VectorIndexIdentity {
    pub name: String,
    pub version: String,
    pub column_id: ColumnId,
}

/// Loads legacy Parquet components as bytes sharing the decoded value buffers.
#[fastrace::trace]
pub async fn load_vector_index_files(
    operator: Operator,
    settings: &ReadSettings,
    column_names: &[String],
    location: &str,
) -> Result<Vec<Bytes>> {
    load_index_files::<VectorIndexLoadSpec>(operator, settings, column_names, location)
        .await?
        .into_iter()
        .map(legacy_component_bytes)
        .collect()
}

fn legacy_component_bytes(column: Column) -> Result<Bytes> {
    let binary = column
        .as_binary()
        .filter(|binary| binary.len() == 1)
        .ok_or_else(|| ErrorCode::StorageOther("Expected a single-row binary HNSW component"))?;
    // Keep Parquet's value buffer alive without copying, including sliced columns
    // whose only row does not cover the entire buffer.
    Ok(Bytes::from_owner(binary.data().clone()).slice_ref(binary.value(0)))
}

/// Checks that `location` holds the current version of `identity`. A bundle is
/// selected from `BlockMeta` by version, so only a legacy combined file (which may
/// hold an outdated version of this index) needs its metadata checked.
pub(super) async fn check_index_current(
    operator: &Operator,
    location: &IndexFileLocation,
    identity: &VectorIndexIdentity,
) -> Result<()> {
    let IndexFileLocation::Legacy { path } = location else {
        return Ok(());
    };
    let meta = load_vector_index_meta(operator.clone(), path).await?;
    let current = matches!(meta.as_ref(), VectorIndexMeta::Legacy(meta)
        if meta.metadata.get(&identity.name) == Some(&identity.version));
    if current {
        Ok(())
    } else {
        Err(ErrorCode::StorageUnavailable(
            "Legacy vector index version mismatch; refresh index",
        ))
    }
}

/// Reads the HNSW components `names` and the column validity (bundles only; legacy
/// files have no NULL information) of `identity`. The legacy version is checked by
/// `check_index_current`.
pub(super) async fn load_hnsw(
    operator: &Operator,
    settings: &ReadSettings,
    location: &IndexFileLocation,
    identity: &VectorIndexIdentity,
    names: &[String],
    row_count: usize,
) -> Result<(Vec<Bytes>, Option<Bytes>)> {
    match location {
        IndexFileLocation::Legacy { path } => {
            let data = load_vector_index_files(operator.clone(), settings, names, path).await?;
            Ok((data, None))
        }
        IndexFileLocation::Bundle { path, size } => {
            let footer = load_footer(operator, path, *size, identity, row_count).await?;
            // The bitmap exists only when the column has NULLs.
            let validity = Some(vector_validity_name(identity.column_id))
                .filter(|name| footer.components.contains_key(name));
            let mut selected = names.to_vec();
            selected.extend(validity.iter().cloned());
            let mut data =
                load_bundle_components(operator.clone(), settings, path, &footer, &selected)
                    .await?;
            // The validity was requested last.
            let validity = validity.and_then(|_| data.pop());
            if validity
                .as_ref()
                .is_some_and(|bytes| bytes.len() != row_count.div_ceil(8))
            {
                return Err(ErrorCode::StorageOther(
                    "Vector validity size does not match the block",
                ));
            }
            Ok((data, validity))
        }
    }
}

/// FP16 refine data of one column, opened for page reads.
pub(super) struct RefineData {
    operator: Operator,
    path: String,
    name: String,
    entry: VectorBundleEntry,
    row_count: u64,
    /// Read granularity, chosen by the reader: FP16 rows are plain and fixed-width.
    pub rows_per_page: usize,
    pub dimension: usize,
}

impl RefineData {
    pub async fn open(
        operator: &Operator,
        location: &IndexFileLocation,
        identity: &VectorIndexIdentity,
        row_count: usize,
    ) -> Result<Self> {
        let IndexFileLocation::Bundle { path, size } = location else {
            return Err(ErrorCode::StorageUnavailable(
                "Legacy SQ8 index has no FP16 data; refresh the vector index before enabling refinement",
            ));
        };
        let footer = load_footer(operator, path, *size, identity, row_count).await?;
        let name = vector_fp16_data_name(identity.column_id);
        let entry = footer.components.get(&name).ok_or_else(|| {
            ErrorCode::StorageUnavailable("FP16 refine store missing; refresh vector index")
        })?;
        Ok(Self {
            operator: operator.clone(),
            path: path.clone(),
            entry: entry.clone(),
            name,
            row_count: row_count as u64,
            rows_per_page: VECTOR_REFINE_PAGE_ROWS,
            dimension: entry.dimension(row_count as u64)? as usize,
        })
    }

    pub fn page_row_range(&self, page: usize) -> Result<Range<u64>> {
        let rows_per_page = self.rows_per_page as u64;
        let start = (page as u64)
            .checked_mul(rows_per_page)
            .filter(|start| *start < self.row_count)
            .ok_or_else(|| ErrorCode::StorageOther("FP16 page ordinal out of bounds"))?;
        Ok(start..start.saturating_add(rows_per_page).min(self.row_count))
    }

    /// Reads the whole FP16 data in one request, checked against its checksum and
    /// not cached: a full scan would evict the pages that TopN refinement reuses.
    pub async fn read_all(&self, settings: &ReadSettings) -> Result<Bytes> {
        let requests = vec![(self.name.clone(), self.entry.component_read())];
        let mut data =
            load_bundle_reads(self.operator.clone(), settings, &self.path, requests, None).await?;
        data.pop()
            .ok_or_else(|| ErrorCode::Internal("Missing FP16 component read"))
    }

    /// The read of `page`, named for the cache by its byte range in the file, so
    /// pages cached under another page size are not reused.
    pub(super) fn page_request(&self, page: usize) -> Result<(String, VectorBundlePageRead)> {
        let read = self
            .entry
            .row_range_read(self.page_row_range(page)?, self.row_count)?;
        let name = format!(
            "{}#bytes={}..{}",
            self.name,
            read.offset,
            read.offset + read.length
        );
        Ok((name, read))
    }

    /// Reads `pages` through the refine data cache.
    pub async fn read_pages(&self, settings: &ReadSettings, pages: &[usize]) -> Result<Vec<Bytes>> {
        let requests = pages
            .iter()
            .map(|&page| self.page_request(page))
            .collect::<Result<Vec<_>>>()?;
        let cache = FuseCaches::instance().vector_refine_pages.clone();
        load_bundle_reads(
            self.operator.clone(),
            settings,
            &self.path,
            requests,
            Some(cache),
        )
        .await
    }
}

/// The footer of a bundle, checked to belong to `identity` and to a block of
/// `row_count` rows.
async fn load_footer(
    operator: &Operator,
    path: &str,
    size: u64,
    identity: &VectorIndexIdentity,
    row_count: usize,
) -> Result<Arc<VectorBundleFooter>> {
    let footer = load_bundle_footer(operator.clone(), path, size).await?;
    if footer.index_name != identity.name || footer.index_version != identity.version {
        return Err(ErrorCode::StorageOther(
            "Vector bundle index identity mismatch",
        ));
    }
    if footer.row_count != row_count as u64 {
        return Err(ErrorCode::StorageOther(
            "Vector bundle row count differs from block metadata",
        ));
    }
    Ok(footer)
}
