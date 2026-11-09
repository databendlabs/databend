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

use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use bytes::Bytes;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorColumn;
use databend_common_storages_fuse::io::read::BundleReadCache;
use databend_common_storages_fuse::io::read::full_read_settings;
use databend_common_storages_fuse::io::read::load_bundle_reads;
use databend_storages_common_cache::CacheFill;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskCache;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_cache::kinds::vector;
use databend_storages_common_index::VectorBundleBuilder;
use databend_storages_common_index::VectorBundleFooter;
use databend_storages_common_index::VectorBundlePageRead;
use databend_storages_common_index::vector_fp16_data_name;
use databend_storages_common_io::ReadSettings;
use opendal::Operator;
use opendal::raw::Access;
use opendal::raw::Layer;
use opendal::raw::LayeredAccess;
use opendal::raw::OpList;
use opendal::raw::OpRead;
use opendal::raw::OpWrite;
use opendal::raw::RpDelete;
use opendal::raw::RpList;
use opendal::raw::RpRead;
use opendal::raw::RpWrite;

/// Bundle reads run on the global IO runtime.
fn init_globals() -> Result<()> {
    static INIT: OnceLock<Result<()>> = OnceLock::new();
    INIT.get_or_init(|| {
        GlobalInstance::init_production();
        GlobalIORuntime::init(1)?;
        Ok(())
    })
    .clone()
}

/// Records the byte range of every storage read.
#[derive(Clone, Debug, Default)]
struct RecordingLayer {
    ranges: Arc<Mutex<Vec<Range<u64>>>>,
}

impl<A: Access> Layer<A> for RecordingLayer {
    type LayeredAccess = RecordingAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        RecordingAccessor {
            inner,
            ranges: self.ranges.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct RecordingAccessor<A: Access> {
    inner: A,
    ranges: Arc<Mutex<Vec<Range<u64>>>>,
}

impl<A: Access> LayeredAccess for RecordingAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        let range = args.range();
        if let Some(size) = range.size() {
            self.ranges
                .lock()
                .unwrap()
                .push(range.offset()..range.offset() + size);
        }
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }
}

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

/// No merging: one storage request per range.
const UNMERGED: ReadSettings = ReadSettings {
    max_gap_size: 0,
    max_range_size: 0,
    parquet_fast_read_bytes: 0,
};

async fn read(
    storage: &DiskCachedStorage,
    path: &str,
    requests: &[(String, VectorBundlePageRead)],
    kind: CacheKind,
    disk: CacheFill,
) -> Result<Vec<Bytes>> {
    read_with(storage, &UNMERGED, path, requests, kind, disk).await
}

async fn read_with(
    storage: &DiskCachedStorage,
    settings: &ReadSettings,
    path: &str,
    requests: &[(String, VectorBundlePageRead)],
    kind: CacheKind,
    disk: CacheFill,
) -> Result<Vec<Bytes>> {
    let cache = BundleReadCache {
        kind,
        disk,
        memory: None,
    };
    load_bundle_reads(storage.clone(), settings, path, requests.to_vec(), cache).await
}

/// HNSW components and FP16 pages read without a memory cache are kept in the disk cache
/// under their kinds: once read, they are served from disk when storage no longer has
/// them. A read that bypasses the disk cache keeps nothing there.
#[tokio::test(flavor = "multi_thread")]
async fn test_reads_without_memory_cache_fill_disk() -> Result<()> {
    init_globals()?;
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

/// A full read of a component with every other page on disk reads the missing pages
/// with one storage request; with the default merge settings, each is one request.
#[tokio::test(flavor = "multi_thread")]
async fn test_full_read_merges_missing_pages() -> Result<()> {
    init_globals()?;
    let bundle = bundle()?;
    let footer = VectorBundleFooter::from_bundle(&bundle)?;
    let fp16 = &footer.components[&vector_fp16_data_name(0)];
    let layer = RecordingLayer::default();
    let ranges = layer.ranges.clone();
    let operator = Operator::new(opendal::services::Memory::default())?
        .layer(layer)
        .finish();
    let dir = tempfile::TempDir::new()?;
    let disk = DiskCache::new(dir.path(), 1 << 30, [1 << 20, 1 << 20, 1 << 20], false)?;
    let storage = DiskCachedStorage::with_disk(operator.clone(), Some(Arc::new(disk)));
    let refine = CacheKind::of::<vector::Refine>();
    let pages = (0..ROWS as u64)
        .step_by(10)
        .map(|start| {
            let read = fp16.row_range_read(start..start + 10, ROWS as u64)?;
            Ok((format!("page {start}"), read))
        })
        .collect::<Result<Vec<_>>>()?;
    let cached = pages.iter().step_by(2).cloned().collect::<Vec<_>>();
    let defaults = ReadSettings {
        max_gap_size: 48,
        max_range_size: 512 * 1024,
        parquet_fast_read_bytes: 0,
    };

    let mut requests = Vec::new();
    for (path, settings) in [
        ("pages/h0.index", defaults),
        ("full/h0.index", full_read_settings(&defaults, fp16.length)),
    ] {
        operator.write(path, bundle.clone()).await?;
        read(&storage, path, &cached, refine, CacheFill::ReadWrite).await?;
        ranges.lock().unwrap().clear();
        let data = read_with(
            &storage,
            &settings,
            path,
            &pages,
            refine,
            CacheFill::ReadWrite,
        )
        .await?;
        assert_eq!(fp16.join_rows(data)?.len() as u64, fp16.raw_length);
        requests.push(ranges.lock().unwrap().len());
    }
    assert_eq!(requests, [pages.len() - cached.len(), 1]);
    Ok(())
}
