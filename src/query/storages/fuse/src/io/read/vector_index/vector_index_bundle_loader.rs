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
use databend_storages_common_cache::MemCache;
use databend_storages_common_index::VECTOR_BUNDLE_INITIAL_FOOTER_READ_SIZE;
use databend_storages_common_index::VECTOR_BUNDLE_TRAILER_SIZE;
use databend_storages_common_index::VectorBundleFooter;
use databend_storages_common_index::VectorBundlePageRead;
use databend_storages_common_index::VectorBundleTrailer;
use databend_storages_common_index::VectorIndexFile;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_io::MergeIOReader;
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
) -> Result<Arc<VectorBundleFooter>> {
    // Parsed footers live in the vector index meta cache, keyed by location (bundle
    // and legacy files have different paths), so hits need no I/O or parsing.
    let cache = &FuseCaches::instance().index_metas.vector;
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
    let tail = operator
        .read_with(location)
        .range(tail_start..size)
        .await?
        .to_bytes();
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
        let bytes = operator.read_with(location).range(range).await?.to_bytes();
        read_bytes += bytes.len();
        bytes
    };
    metrics_inc_block_vector_index_read_bytes(read_bytes as u64);
    let footer = Arc::new(VectorBundleFooter::decode(&footer_bytes, &trailer)?);
    cache.fill(location, Arc::new(VectorIndexMeta::Bundle(footer.clone())));
    Ok(footer)
}

/// Reads whole components by name, without caching the bytes.
pub async fn load_bundle_components(
    operator: Operator,
    settings: &ReadSettings,
    location: &str,
    footer: &VectorBundleFooter,
    names: &[String],
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
    load_bundle_reads(operator, settings, location, requests, None).await
}

/// Reads byte ranges (whole components or pages), named for the cache key. With a
/// `cache`, decoded bytes are looked up and stored there.
pub async fn load_bundle_reads(
    operator: Operator,
    settings: &ReadSettings,
    location: &str,
    requests: Vec<(String, VectorBundlePageRead)>,
    cache: Option<MemCache<VectorIndexFile>>,
) -> Result<Vec<Bytes>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    let location = location.to_owned();
    let settings = *settings;
    GlobalIORuntime::instance()
        .spawn(async move {
            load_bundle_reads_inner(operator, &settings, &location, &requests, cache).await
        })
        .await
        .map_err(|error| {
            ErrorCode::TokioError(format!("Vector component read task failed: {error}"))
        })?
}

async fn load_bundle_reads_inner(
    operator: Operator,
    settings: &ReadSettings,
    location: &str,
    requests: &[(String, VectorBundlePageRead)],
    cache: Option<MemCache<VectorIndexFile>>,
) -> Result<Vec<Bytes>> {
    let start = std::time::Instant::now();
    let mut output = vec![None; requests.len()];
    let mut ranges = Vec::new();
    for (i, (name, entry)) in requests.iter().enumerate() {
        let hit = cache.as_ref().and_then(|cache| {
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
        let read = MergeIOReader::merge_io_read(settings, operator, location, &ranges).await?;
        metrics_inc_block_vector_index_read_bytes(
            ranges
                .iter()
                .map(|(_, range)| range.end - range.start)
                .sum(),
        );
        for (i, (chunk, range)) in &read.columns_chunk_offsets {
            let owner = read.owner_memory.get_chunk(*chunk, &read.block_path)?;
            // Keep uncached reads zero-copy when the merged buffer is contiguous.
            let bytes = owner.slice(range.clone()).to_bytes();
            let index = *i as usize;
            let (name, entry) = &requests[index];
            let raw = entry.decode(bytes)?;
            let raw = if let Some(cache) = cache.as_ref().filter(|cache| cache.is_enabled()) {
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
