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

//! Bundle container: footer and component reads by name. Only the footer type and
//! its meta cache are vector specific; which components to read is decided by
//! `vector_index_loader`.

use std::sync::Arc;

use bytes::Bytes;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::metrics_inc_block_vector_index_read_bytes;
use databend_common_metrics::storage::metrics_inc_block_vector_index_read_milliseconds;
use databend_storages_common_cache::CacheFill;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::kinds::vector;
use databend_storages_common_index::VECTOR_BUNDLE_INITIAL_FOOTER_READ_SIZE;
use databend_storages_common_index::VECTOR_BUNDLE_TRAILER_SIZE;
use databend_storages_common_index::VectorBundleFooter;
use databend_storages_common_index::VectorBundlePageRead;
use databend_storages_common_index::VectorBundleTrailer;
use databend_storages_common_index::VectorIndexFile;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_io::ReadSettings;
use opendal::Operator;

use crate::io::FuseCaches;

/// Cache key of one component (or page) of a bundle. Bundle locations are unique
/// immutable objects, so the location and component name identify the bytes; no
/// checksum is needed.
fn component_cache_key(location: &str, component: &str) -> String {
    format!("{location}:{component}")
}

/// Loads the footer of a bundle of `size` bytes (recorded in `BlockMeta`). A cache
/// miss costs one read of the file end.
pub async fn load_bundle_footer(
    operator: Operator,
    location: &str,
    size: u64,
    policy: CachePolicy,
) -> Result<Arc<VectorBundleFooter>> {
    // Parsed footers live in the vector index meta cache, keyed by location (bundle
    // and legacy files have different paths), so hits need no I/O or parsing.
    let cache = &FuseCaches::instance()
        .index_metas
        .vector
        .with_policy(policy);
    if let Some(meta) = cache.lookup(location) {
        return match meta.as_ref() {
            VectorIndexMeta::Bundle(footer) => Ok(footer.clone()),
            VectorIndexMeta::Legacy(_) => Err(ErrorCode::StorageOther(
                "Cached vector index meta is not a bundle footer",
            )),
        };
    }
    if size < VECTOR_BUNDLE_TRAILER_SIZE as u64 {
        return Err(ErrorCode::StorageOther("Truncated vector bundle"));
    }
    // One read of the file end usually covers both the footer and the trailer;
    // only a footer larger than the initial read needs a second request.
    let tail_start = size.saturating_sub(VECTOR_BUNDLE_INITIAL_FOOTER_READ_SIZE as u64);
    let expected_tail_len = size - tail_start;
    // The tail and, when larger, the footer are units of the disk cache.
    let storage = DiskCachedStorage::new(operator);
    let meta_kind = CacheKind::of::<vector::Meta>();
    let fill = cache.policy().disk;
    let tail = storage
        .read(meta_kind, location, Some(tail_start..size), fill)
        .await?;
    // Both the trailer and an in-tail footer are sliced using the recorded size.
    if tail.len() as u64 != expected_tail_len {
        return Err(ErrorCode::StorageOther(format!(
            "Vector bundle tail read length mismatch at {location}: expected {expected_tail_len} bytes, got {}",
            tail.len()
        )));
    }
    let mut read_bytes = tail.len();
    let trailer =
        VectorBundleTrailer::parse(&tail[tail.len() - VECTOR_BUNDLE_TRAILER_SIZE..], size)?;
    let range = trailer.footer.clone();
    let footer_bytes = if range.start >= tail_start {
        tail.slice((range.start - tail_start) as usize..(range.end - tail_start) as usize)
    } else {
        let bytes = storage.read(meta_kind, location, Some(range), fill).await?;
        read_bytes += bytes.len();
        bytes
    };
    metrics_inc_block_vector_index_read_bytes(read_bytes as u64);
    let footer = Arc::new(VectorBundleFooter::decode(&footer_bytes, &trailer)?);
    cache.fill(location, Arc::new(VectorIndexMeta::Bundle(footer.clone())));
    Ok(footer)
}

/// The caches a bundle read goes through: the disk cache of object bytes, as `kind` with
/// `disk`, and the memory cache of decoded bytes, if any.
pub struct BundleReadCache {
    pub kind: CacheKind,
    pub disk: CacheFill,
    pub memory: Option<MemCache<VectorIndexFile>>,
}

/// Reads whole HNSW components by name. They are decoded into the HNSW cache, so their bytes
/// are not kept in memory, but they are kept in the disk cache.
pub async fn load_bundle_components(
    operator: Operator,
    settings: &ReadSettings,
    location: &str,
    footer: &VectorBundleFooter,
    names: &[String],
    policy: CachePolicy,
) -> Result<Vec<Bytes>> {
    let requests = names
        .iter()
        .map(|name| {
            let entry = footer.components.get(name).ok_or_else(|| {
                ErrorCode::StorageUnavailable(format!(
                    "Vector component {name} missing; refresh index"
                ))
            })?;
            Ok((name.clone(), entry.component_read()))
        })
        .collect::<Result<Vec<_>>>()?;
    let cache = BundleReadCache {
        kind: CacheKind::of::<vector::Index>(),
        disk: policy.disk,
        memory: None,
    };
    let storage = DiskCachedStorage::new(operator);
    load_bundle_reads(storage, settings, location, requests, cache).await
}

/// Reads byte ranges (whole components or pages), named for the memory cache key,
/// through the caches of `cache`.
pub async fn load_bundle_reads(
    storage: DiskCachedStorage,
    settings: &ReadSettings,
    location: &str,
    requests: Vec<(String, VectorBundlePageRead)>,
    cache: BundleReadCache,
) -> Result<Vec<Bytes>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    let location = location.to_owned();
    let settings = *settings;
    GlobalIORuntime::instance()
        .spawn(async move {
            load_bundle_reads_inner(&storage, &settings, &location, &requests, cache).await
        })
        .await
        .map_err(|error| {
            ErrorCode::TokioError(format!("Vector component read task failed: {error}"))
        })?
}

async fn load_bundle_reads_inner(
    storage: &DiskCachedStorage,
    settings: &ReadSettings,
    location: &str,
    requests: &[(String, VectorBundlePageRead)],
    cache: BundleReadCache,
) -> Result<Vec<Bytes>> {
    let start = std::time::Instant::now();
    let mut output = vec![None; requests.len()];
    let mut ranges = Vec::new();
    for (i, (name, entry)) in requests.iter().enumerate() {
        let hit = cache.memory.as_ref().and_then(|cache| {
            cache.lookup_sized(&component_cache_key(location, name), entry.raw_length)
        });
        if entry.length == 0 {
            output[i] = Some(entry.decode(Bytes::new())?);
        } else if let Some(hit) = hit {
            output[i] = Some(hit.data.clone());
        } else {
            ranges.push((i as u32, entry.range()?));
        }
    }
    if !ranges.is_empty() {
        let read = storage
            .read_units(cache.kind, location, &ranges, cache.disk, settings)
            .await?;
        metrics_inc_block_vector_index_read_bytes(
            ranges
                .iter()
                .map(|(_, range)| range.end - range.start)
                .sum(),
        );
        for (i, _) in &ranges {
            // Keep uncached reads zero-copy when the merged buffer is contiguous.
            let bytes = read.unit(*i).ok_or_else(|| {
                ErrorCode::Internal(format!("vector page {i} of {location} was not read"))
            })?;
            let index = *i as usize;
            let (name, entry) = &requests[index];
            let raw = entry.decode(bytes)?;
            let raw = if let Some(cache) = cache.memory.as_ref().filter(|cache| cache.is_enabled())
            {
                // The cache accounts for the page's byte length, not the merged
                // buffer backing a Bytes slice. Detach before caching so a single
                // retained page cannot pin the entire merged read. Return the same
                // owned bytes to the caller as well.
                let page = Bytes::copy_from_slice(&raw);
                cache.fill(
                    &component_cache_key(location, name),
                    VectorIndexFile::create(name.clone(), page.clone()),
                );
                page
            } else {
                raw
            };
            output[index] = Some(raw);
        }
    }
    metrics_inc_block_vector_index_read_milliseconds(start.elapsed().as_millis() as u64);
    output
        .into_iter()
        .map(|bytes| bytes.ok_or_else(|| ErrorCode::StorageOther("Missing vector range result")))
        .collect()
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Column;
    use databend_common_expression::types::F32;
    use databend_common_expression::types::VectorColumn;
    use databend_storages_common_cache::DiskCache;
    use databend_storages_common_index::VectorBundleBuilder;
    use databend_storages_common_index::vector_fp16_data_name;

    use super::*;
    use crate::test_utils::init_test_globals;

    const ROWS: usize = 100;
    const DIM: usize = 4;
    const LINKS: &str = "0-l2_graph_links";

    fn bundle() -> Result<Vec<u8>> {
        let values = (0..ROWS * DIM)
            .map(|i| F32::from(i as f32 * 0.25))
            .collect::<Vec<_>>();
        let column = Column::Vector(VectorColumn::Float32((values.into(), DIM)));
        let mut builder = VectorBundleBuilder::new("vi".to_owned(), "v1".to_owned(), ROWS as u64);
        builder.add_hnsw(LINKS.to_owned(), &[3u8; 4096])?;
        builder.add_fp16_data(0, &column)?;
        builder.finish()
    }

    async fn read(
        storage: &DiskCachedStorage,
        path: &str,
        requests: &[(String, VectorBundlePageRead)],
        kind: CacheKind,
        disk: CacheFill,
    ) -> Result<Vec<Bytes>> {
        let settings = ReadSettings {
            max_gap_size: 0,
            max_range_size: 0,
            parquet_fast_read_bytes: 0,
        };
        let cache = BundleReadCache {
            kind,
            disk,
            memory: None,
        };
        load_bundle_reads(storage.clone(), &settings, path, requests.to_vec(), cache).await
    }

    /// HNSW components and FP16 pages read without a memory cache are kept in the disk cache
    /// under their kinds: once read, they are served from disk when storage no longer has
    /// them. A read that bypasses the disk cache keeps nothing there.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_reads_without_memory_cache_fill_disk() -> Result<()> {
        init_test_globals()?;
        let bundle = bundle()?;
        let footer = VectorBundleFooter::from_bundle(&bundle)?;
        let operator = Operator::via_iter(opendal::Scheme::Memory, [])?;
        let dir = tempfile::TempDir::new()?;
        let disk = DiskCache::new(dir.path(), 1 << 30, [1 << 20, 1 << 20, 1 << 20], false)?;
        let storage = DiskCachedStorage::with_disk(operator.clone(), Some(Arc::new(disk)));
        let hnsw = CacheKind::of::<vector::Index>();
        let refine = CacheKind::of::<vector::Refine>();

        let links = footer.components[LINKS].component_read();
        let fp16 = &footer.components[&vector_fp16_data_name(0)];
        let components = vec![(LINKS.to_owned(), links.clone())];
        let pages = [0..50, 50..ROWS as u64]
            .into_iter()
            .map(|rows| {
                Ok((
                    format!("page {rows:?}"),
                    fp16.row_range_read(rows, ROWS as u64)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        // Bypassing the disk cache keeps nothing.
        let bypassed = "bypassed/h0.index";
        operator.write(bypassed, bundle.clone()).await?;
        read(&storage, bypassed, &components, hnsw, CacheFill::Bypass).await?;
        assert!(!storage.holds(hnsw, bypassed, Some(links.range()?), CacheFill::ReadWrite));

        let path = "cached/h0.index";
        operator.write(path, bundle).await?;
        let first = read(&storage, path, &components, hnsw, CacheFill::ReadWrite).await?;
        let first_pages = read(&storage, path, &pages, refine, CacheFill::ReadWrite).await?;
        assert!(storage.holds(hnsw, path, Some(links.range()?), CacheFill::ReadWrite));
        for (_, page) in &pages {
            assert!(storage.holds(refine, path, Some(page.range()?), CacheFill::ReadWrite));
        }

        // Served from disk once storage has lost the object.
        operator.delete(path).await?;
        let again = read(&storage, path, &components, hnsw, CacheFill::ReadWrite).await?;
        assert_eq!(again, first);
        assert_eq!(&again[0][..], &[3u8; 4096][..]);
        let again_pages = read(&storage, path, &pages, refine, CacheFill::ReadWrite).await?;
        assert_eq!(again_pages, first_pages);
        // The pages join into the whole FP16 data, checked against its checksum.
        assert_eq!(fp16.join_rows(again_pages)?.len() as u64, fp16.raw_length);
        assert!(
            read(&storage, path, &components, hnsw, CacheFill::Bypass)
                .await
                .is_err()
        );
        Ok(())
    }
}
