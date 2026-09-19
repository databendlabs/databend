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

// Portions of this module are derived from Quickwit's static directory cache.
// Copyright 2021-Present Datadog, Inc.
// Source revision: 4b4c6442cc88321ce1206228f165be40e09500f0.
// Modified by Datafuse Labs to store index-open slices directly in the bundle footer.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tantivy::Directory;
use tantivy::HasLen;
use tantivy::Index;
use tantivy::IndexReader;
use tantivy::ReloadPolicy;
use tantivy::directory::FileHandle;
use tantivy::directory::FileSlice;
use tantivy::directory::OwnedBytes;
use tantivy::directory::error::OpenReadError;

use super::bundle::BundleOpenSlice;
use super::bundle::InvertedIndexBundleFooter;
use super::bundle::MANAGED_JSON_PATH;
use super::bundle::META_JSON_PATH;
use super::debug_proxy::DebugProxyDirectory;
use super::read_only_directory;

fn merge_ranges(mut ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
    ranges.sort_unstable_by_key(|range| range.start);
    let mut merged: Vec<Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.is_empty() {
            continue;
        }
        if let Some(last) = merged.last_mut() {
            if range.start <= last.end {
                last.end = last.end.max(range.end);
                continue;
            }
        }
        merged.push(range);
    }
    merged
}

/// Observes the opaque logical ranges Tantivy reads while opening an index.
///
/// The two index-level JSON files are excluded because their complete bytes are stored separately
/// in the bundle footer. Databend does not interpret any bytes collected by this function.
///
/// These ranges are specific to the Tantivy revision that built the bundle. If a later Tantivy
/// upgrade reads additional ranges during `Index::open` / `SegmentReader` setup, existing footers
/// will miss them and synchronous search will fail with `WouldBlock`. Bump
/// [`super::bundle::INVERTED_INDEX_FILE_FORMAT_VERSION`] and rebuild indexes in that case.
pub fn collect_index_open_slices<D: Directory + Clone>(
    directory: D,
) -> tantivy::Result<BTreeMap<PathBuf, Vec<BundleOpenSlice>>> {
    let proxy = DebugProxyDirectory::wrap(directory.clone());
    let index = Index::open(proxy.clone())?;
    let reader: IndexReader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    for (field, entry) in index.schema().fields() {
        if !entry.is_indexed() {
            continue;
        }
        for segment_reader in searcher.segment_readers() {
            let _ = segment_reader.inverted_index(field)?;
        }
    }

    let mut observed: HashMap<PathBuf, Vec<Range<usize>>> = HashMap::new();
    for operation in proxy.drain_read_operations() {
        if operation.path == Path::new(MANAGED_JSON_PATH)
            || operation.path == Path::new(META_JSON_PATH)
        {
            continue;
        }
        let end = operation
            .offset
            .checked_add(operation.num_bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "open slice overflow"))?;
        observed
            .entry(operation.path)
            .or_default()
            .push(operation.offset..end);
    }

    let mut open_slices = BTreeMap::new();
    for (path, ranges) in observed {
        let file = directory.open_read(&path)?;
        let mut slices = Vec::new();
        for range in merge_ranges(ranges) {
            if range.end > file.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "observed Tantivy read exceeds logical file length",
                )
                .into());
            }
            slices.push(BundleOpenSlice {
                range: u64::try_from(range.start).map_err(io::Error::other)?
                    ..u64::try_from(range.end).map_err(io::Error::other)?,
                bytes: Arc::from(file.read_bytes_slice(range)?.as_ref()),
            });
        }
        if !slices.is_empty() {
            open_slices.insert(path, slices);
        }
    }
    Ok(open_slices)
}

struct FooterFileHandle {
    underlying: FileSlice,
    open_slices: Arc<[BundleOpenSlice]>,
    len: usize,
}

impl FooterFileHandle {
    fn read_open_slice(&self, range: Range<usize>) -> Option<OwnedBytes> {
        if range.is_empty() {
            return Some(OwnedBytes::empty());
        }
        let start = u64::try_from(range.start).ok()?;
        let end = u64::try_from(range.end).ok()?;
        let slice = self
            .open_slices
            .iter()
            .find(|slice| slice.range.start <= start && slice.range.end >= end)?;
        let start = usize::try_from(start - slice.range.start).ok()?;
        Some(OwnedBytes::new(slice.bytes.clone()).slice(start..start + range.len()))
    }
}

impl fmt::Debug for FooterFileHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("FooterFileHandle")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl HasLen for FooterFileHandle {
    fn len(&self) -> usize {
        self.len
    }
}

#[async_trait]
impl FileHandle for FooterFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "inverted-index logical range exceeds file length",
            ));
        }
        if let Some(bytes) = self.read_open_slice(range.clone()) {
            return Ok(bytes);
        }
        self.underlying.read_bytes_slice(range)
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "inverted-index logical range exceeds file length",
            ));
        }
        if let Some(bytes) = self.read_open_slice(range.clone()) {
            return Ok(bytes);
        }
        self.underlying.read_bytes_slice_async(range).await
    }
}

/// Directory that serves index-level files and synchronous index-open slices from the footer.
///
/// All other reads are delegated to the search-scoped pin beneath it. A synchronous miss
/// therefore remains fail-closed until async warmup has pinned the required range.
#[derive(Clone)]
pub struct FooterDirectory {
    inner: Arc<FooterDirectoryInner>,
}

struct FooterDirectoryInner {
    underlying: Box<dyn Directory>,
    footer: InvertedIndexBundleFooter,
}

impl FooterDirectory {
    pub fn new<D: Directory>(underlying: D, footer: InvertedIndexBundleFooter) -> Self {
        Self {
            inner: Arc::new(FooterDirectoryInner {
                underlying: Box::new(underlying),
                footer,
            }),
        }
    }

    fn inline_file(&self, path: &Path) -> Option<OwnedBytes> {
        if path == Path::new(MANAGED_JSON_PATH) {
            Some(OwnedBytes::new(self.inner.footer.managed_json.clone()))
        } else if path == Path::new(META_JSON_PATH) {
            Some(OwnedBytes::new(self.inner.footer.meta_json.clone()))
        } else {
            None
        }
    }
}

impl fmt::Debug for FooterDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("FooterDirectory")
            .field("files", &self.inner.footer.file_ranges.files.len())
            .finish_non_exhaustive()
    }
}

impl Directory for FooterDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        if let Some(bytes) = self.inline_file(path) {
            return Ok(Arc::new(bytes));
        }
        // Inline and external files both resolve through the underlying directory by path;
        // only the logical length comes from the footer.
        let file_len = self
            .inner
            .footer
            .file_len(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        let len = usize::try_from(file_len).map_err(|error| {
            OpenReadError::wrap_io_error(io::Error::other(error), path.to_path_buf())
        })?;
        let underlying =
            FileSlice::new_with_num_bytes(self.inner.underlying.get_file_handle(path)?, len);
        Ok(Arc::new(FooterFileHandle {
            underlying,
            open_slices: self
                .inner
                .footer
                .open_slices
                .get(path)
                .cloned()
                .unwrap_or_else(|| Arc::<[BundleOpenSlice]>::from([])),
            len,
        }))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if let Some(bytes) = self.inline_file(path) {
            return Ok(bytes.as_ref().to_vec());
        }
        let file = self.get_file_handle(path)?;
        file.read_bytes(0..file.len())
            .map(|bytes| bytes.as_ref().to_vec())
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(path == Path::new(MANAGED_JSON_PATH)
            || path == Path::new(META_JSON_PATH)
            || self.inner.footer.file_len(path).is_some())
    }

    read_only_directory!();
}

#[cfg(test)]
mod tests {
    use tantivy::IndexSettings;
    use tantivy::TantivyDocument;
    use tantivy::directory::RamDirectory;
    use tantivy::schema::Schema;
    use tantivy::schema::TEXT;

    use super::super::bundle::BundleFileRanges;
    use super::super::search_pin::SearchPinDirectory;
    use super::*;

    #[derive(Clone, Debug)]
    struct SyncWouldBlockDirectory {
        file_ranges: BundleFileRanges,
    }

    #[derive(Debug)]
    struct SyncWouldBlockFileHandle {
        path: PathBuf,
        len: usize,
    }

    impl HasLen for SyncWouldBlockFileHandle {
        fn len(&self) -> usize {
            self.len
        }
    }

    #[async_trait]
    impl FileHandle for SyncWouldBlockFileHandle {
        fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "raw-region range {}..{} for {} must be served from the footer",
                    range.start,
                    range.end,
                    self.path.display()
                ),
            ))
        }

        async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
            self.read_bytes(range)
        }
    }

    impl Directory for SyncWouldBlockDirectory {
        fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
            let range = self
                .file_ranges
                .get(path)
                .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
            let len = usize::try_from(range.end - range.start).map_err(|error| {
                OpenReadError::wrap_io_error(io::Error::other(error), path.to_path_buf())
            })?;
            Ok(Arc::new(SyncWouldBlockFileHandle {
                path: path.to_path_buf(),
                len,
            }))
        }

        fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
            let file = self.get_file_handle(path)?;
            file.read_bytes(0..file.len())
                .map(|bytes| bytes.as_ref().to_vec())
                .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))
        }

        fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
            Ok(self.file_ranges.contains(path))
        }

        read_only_directory!();
    }

    #[test]
    fn test_footer_directory_serves_inline_files_and_open_slices() {
        let underlying = RamDirectory::default();
        underlying
            .atomic_write(Path::new("segment.term"), b"abcdef")
            .unwrap();
        let search_pin = SearchPinDirectory::new(Arc::new(underlying));
        let bundle_bytes = InvertedIndexBundleFooter::build(
            [("segment.term", b"abcdef".as_slice())],
            BTreeMap::new(),
            BTreeMap::from([(PathBuf::from("segment.term"), vec![BundleOpenSlice {
                range: 1..5,
                bytes: Arc::from(b"bcde".as_slice()),
            }])]),
            b"managed".to_vec(),
            b"meta".to_vec(),
        )
        .unwrap();
        let footer = InvertedIndexBundleFooter::open(&bundle_bytes).unwrap();
        let directory = FooterDirectory::new(search_pin, footer);

        assert_eq!(
            directory.atomic_read(Path::new(META_JSON_PATH)).unwrap(),
            b"meta"
        );
        let term = directory.open_read(Path::new("segment.term")).unwrap();
        assert_eq!(term.read_bytes_slice(2..4).unwrap().as_ref(), b"cd");
        assert_eq!(term.len(), 6);
        assert_eq!(
            term.read_bytes_slice(0..1).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn test_tantivy_reopens_from_footer_data() {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let raw_directory = RamDirectory::default();
        let index = Index::create(
            raw_directory.clone(),
            schema_builder.build(),
            IndexSettings::default(),
        )
        .unwrap();
        let mut writer: tantivy::IndexWriter<TantivyDocument> = index.writer(15_000_000).unwrap();
        let mut document = TantivyDocument::new();
        document.add_text(text, "hello");
        writer.add_document(document).unwrap();
        writer.commit().unwrap();

        let open_slices = collect_index_open_slices(raw_directory.clone()).unwrap();
        let mut paths = index
            .directory()
            .list_managed_files()
            .into_iter()
            .filter(|path| {
                path != Path::new(MANAGED_JSON_PATH) && path != Path::new(META_JSON_PATH)
            })
            .collect::<Vec<_>>();
        paths.sort_unstable();
        let files = paths
            .into_iter()
            .filter(|path| raw_directory.exists(path).unwrap())
            .map(|path| {
                let bytes = raw_directory.atomic_read(&path).unwrap();
                (path, bytes)
            })
            .collect::<Vec<_>>();
        let managed_json = raw_directory
            .atomic_read(Path::new(MANAGED_JSON_PATH))
            .unwrap();
        let meta_json = raw_directory
            .atomic_read(Path::new(META_JSON_PATH))
            .unwrap();
        let bundle_bytes = super::super::bundle::InvertedIndexBundleFooter::build(
            files,
            BTreeMap::new(),
            open_slices,
            managed_json,
            meta_json,
        )
        .unwrap();
        let footer = super::super::bundle::InvertedIndexBundleFooter::open(&bundle_bytes).unwrap();
        let raw_bundle_directory = SyncWouldBlockDirectory {
            file_ranges: footer.file_ranges.clone(),
        };

        let search_pin = SearchPinDirectory::new(Arc::new(raw_bundle_directory));
        let directory = FooterDirectory::new(search_pin, footer);

        let reopened = Index::open(directory).unwrap();
        let reader = reopened.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = &searcher.segment_readers()[0];
        assert!(segment_reader.inverted_index(text).is_ok());
        // SegmentReader::open only needs the `.fieldnorm` composite footer. Loading a field's
        // payload is search-time IO and is warmed asynchronously, so it is not in open_slices.
        assert!(
            segment_reader
                .fieldnorms_readers()
                .get_inner_file()
                .open_read(text)
                .is_some()
        );
        match segment_reader.fieldnorms_readers().get_field(text) {
            Err(tantivy::TantivyError::IoError(err)) => {
                assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
            }
            Err(err) => panic!(
                "expected WouldBlock when loading fieldnorm payload from footer-only open, got {err:?}"
            ),
            Ok(_) => panic!(
                "expected WouldBlock when loading fieldnorm payload from footer-only open, got Ok"
            ),
        }
    }

    #[test]
    fn test_merge_overlapping_ranges() {
        assert_eq!(merge_ranges(vec![5..10, 0..3, 2..7, 12..13]), vec![
            0..10,
            12..13
        ]);
    }
}
