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

//! Output directory for building one inverted index bundle. Small files stay in a `RamDirectory`;
//! postings and positions that outgrow `stream_threshold` stream to sibling objects
//! `<bundle location>.idx` / `.pos`. Only the bytes Tantivy reads while opening the index stay in
//! memory for those files: the tail (component footers) and the head of every section.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::io::BufWriter;
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use databend_storages_common_io::BLOCKING_WRITE_MAX_CHUNKS;
use databend_storages_common_io::BlockingWrite;
use databend_storages_common_io::OpenDalBlockingWrite;
use databend_storages_common_io::create_blocking_write;
use opendal::Buffer;
use opendal::Operator;
use tantivy::Directory;
use tantivy::HasLen;
use tantivy::directory::AntiCallToken;
use tantivy::directory::DirectoryLock;
use tantivy::directory::FileHandle;
use tantivy::directory::Lock;
use tantivy::directory::OwnedBytes;
use tantivy::directory::RamDirectory;
use tantivy::directory::TerminatingWrite;
use tantivy::directory::WatchCallback;
use tantivy::directory::WatchHandle;
use tantivy::directory::WritePtr;
use tantivy::directory::error::DeleteError;
use tantivy::directory::error::LockError;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::error::OpenWriteError;

use super::bundle::ExternalFile;

/// Files up to this size stay inline; streamed files retain this many trailing bytes.
pub const INVERTED_INDEX_STREAM_THRESHOLD: usize = 4 * 1024 * 1024;

/// Bytes retained from the file start and from after every `flush` of a streamed file.
///
/// Tantivy's `FieldSerializer::close` flushes the postings writer after each field, so a flush
/// marks the start of the next field's section, whose first 8 bytes (`total_num_tokens`) are read
/// synchronously when the field is opened. Reads are matched by exact section offset.
const SECTION_HEAD_RETAIN: usize = 64;

#[derive(Clone)]
pub struct InvertedIndexOutputDirectory {
    inner: Arc<OutputDirectoryInner>,
}

struct OutputDirectoryInner {
    ram: RamDirectory,
    operator: Operator,
    bundle_location: String,
    stream_threshold: usize,
    streamed: Mutex<BTreeMap<PathBuf, StreamedFile>>,
}

struct StreamedFile {
    suffix: String,
    len: u64,
    tail: Buffer,
    heads: BTreeMap<u64, Bytes>,
}

impl InvertedIndexOutputDirectory {
    pub fn new(operator: Operator, bundle_location: String) -> Self {
        Self::with_stream_threshold(operator, bundle_location, INVERTED_INDEX_STREAM_THRESHOLD)
    }

    pub fn with_stream_threshold(
        operator: Operator,
        bundle_location: String,
        stream_threshold: usize,
    ) -> Self {
        Self {
            inner: Arc::new(OutputDirectoryInner {
                ram: RamDirectory::default(),
                operator,
                bundle_location,
                stream_threshold: stream_threshold.max(1),
                streamed: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// Inline files, including `meta.json` and `.managed.json`.
    pub fn ram_directory(&self) -> RamDirectory {
        self.inner.ram.clone()
    }

    /// Files that were streamed to sibling objects.
    pub fn external_files(&self) -> BTreeMap<PathBuf, ExternalFile> {
        let mut files = BTreeMap::new();
        for (path, streamed) in self.inner.streamed.lock().unwrap().iter() {
            files.insert(path.clone(), ExternalFile {
                suffix: streamed.suffix.clone(),
                len: streamed.len,
            });
        }
        files
    }

    #[cfg(test)]
    fn section_head_count(&self, path: &Path) -> usize {
        let streamed = self.inner.streamed.lock().unwrap();
        match streamed.get(path) {
            Some(file) => file.heads.len(),
            None => 0,
        }
    }

    fn stream_suffix(path: &Path) -> Option<String> {
        match path.extension().and_then(|extension| extension.to_str()) {
            Some(extension @ ("idx" | "pos")) => Some(format!(".{extension}")),
            _ => None,
        }
    }
}

impl fmt::Debug for InvertedIndexOutputDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("InvertedIndexOutputDirectory")
            .field("bundle_location", &self.inner.bundle_location)
            .field("streamed", &self.inner.streamed.lock().unwrap().len())
            .finish()
    }
}

impl Directory for InvertedIndexOutputDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let streamed = self.inner.streamed.lock().unwrap();
        match streamed.get(path) {
            Some(file) => Ok(Arc::new(StreamedFileHandle {
                path: path.to_path_buf(),
                len: file.len,
                tail: file.tail.clone(),
                heads: file.heads.clone(),
            })),
            None => self.inner.ram.get_file_handle(path),
        }
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.inner.ram.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        if self.inner.streamed.lock().unwrap().contains_key(path) {
            return Ok(true);
        }
        self.inner.ram.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let Some(suffix) = Self::stream_suffix(path) else {
            return self.inner.ram.open_write(path);
        };
        let write = OutputFileWrite {
            directory: self.inner.clone(),
            path: path.to_path_buf(),
            suffix,
            buffer: Vec::new(),
            pos: 0,
            written: 0,
            heads: BTreeMap::new(),
            head_pending: true,
            head_open: false,
            sink: None,
        };
        Ok(BufWriter::new(Box::new(write)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.inner.ram.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.inner.ram.atomic_write(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.inner.ram.sync_directory()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.ram.watch(watch_callback)
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.inner.ram.acquire_lock(lock)
    }
}

/// Writer for one `.idx` / `.pos` file. `buffer` holds the whole file while it fits inline; once
/// the file outgrows the threshold it turns into a ring of the most recent `stream_threshold`
/// bytes. `head_pending` means the next write starts a section head; `head_open` means the newest
/// head is still being filled.
struct OutputFileWrite {
    directory: Arc<OutputDirectoryInner>,
    path: PathBuf,
    suffix: String,
    buffer: Vec<u8>,
    pos: usize,
    written: u64,
    heads: BTreeMap<u64, Vec<u8>>,
    head_pending: bool,
    head_open: bool,
    sink: Option<OpenDalBlockingWrite>,
}

impl OutputFileWrite {
    fn capacity(&self) -> usize {
        self.directory.stream_threshold
    }

    fn retain_head(&mut self, bytes: &[u8]) {
        if bytes.is_empty() || (!self.head_pending && !self.head_open) {
            return;
        }

        if self.head_pending {
            self.head_pending = false;
            let head = Vec::with_capacity(SECTION_HEAD_RETAIN);
            self.heads.insert(self.written, head);
        }

        let head = self
            .heads
            .last_entry()
            .expect("an open head exists")
            .into_mut();
        let take = (SECTION_HEAD_RETAIN - head.len()).min(bytes.len());
        head.extend_from_slice(&bytes[..take]);
        self.head_open = head.len() < SECTION_HEAD_RETAIN;
    }

    fn retain(&mut self, mut bytes: &[u8]) {
        let capacity = self.capacity();
        if bytes.len() >= capacity {
            bytes = &bytes[bytes.len() - capacity..];
            self.buffer.clear();
            self.pos = 0;
        }
        while !bytes.is_empty() {
            if self.buffer.len() < capacity {
                let grow = (capacity - self.buffer.len()).min(bytes.len());
                self.buffer.extend_from_slice(&bytes[..grow]);
                bytes = &bytes[grow..];
                self.pos = self.buffer.len() % capacity;
            } else {
                let fill = (capacity - self.pos).min(bytes.len());
                self.buffer[self.pos..self.pos + fill].copy_from_slice(&bytes[..fill]);
                bytes = &bytes[fill..];
                self.pos = (self.pos + fill) % capacity;
            }
        }
    }

    fn into_tail(buffer: Vec<u8>, pos: usize, written: u64) -> Buffer {
        let bytes = Bytes::from(buffer);
        if (written as usize) < bytes.len() || bytes.is_empty() {
            let end = (written as usize).min(bytes.len());
            return Buffer::from(bytes.slice(..end));
        }
        Buffer::from(vec![bytes.slice(pos..), bytes.slice(..pos)])
    }

    fn start_streaming(&mut self) -> io::Result<()> {
        let object = format!("{}{}", self.directory.bundle_location, self.suffix);
        let mut sink = create_blocking_write(
            self.directory.operator.clone(),
            object,
            BLOCKING_WRITE_MAX_CHUNKS,
        );
        sink.write_all(&self.buffer)?;
        self.sink = Some(sink);
        Ok(())
    }
}

impl io::Write for OutputFileWrite {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.sink.is_none() && self.buffer.len() + bytes.len() > self.capacity() {
            self.start_streaming()?;
        }
        if let Some(sink) = self.sink.as_mut() {
            sink.write_all(bytes)?;
        }
        self.retain_head(bytes);
        self.retain(bytes);
        self.written += bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.head_pending = true;
        Ok(())
    }
}

impl TerminatingWrite for OutputFileWrite {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        let buffer = std::mem::take(&mut self.buffer);
        let Some(mut sink) = self.sink.take() else {
            return self
                .directory
                .ram
                .atomic_write(&self.path, &buffer[..self.written as usize]);
        };
        sink.close().map_err(io::Error::other)?;
        let tail = Self::into_tail(buffer, self.pos, self.written);
        let mut heads = BTreeMap::new();
        for (offset, head) in std::mem::take(&mut self.heads) {
            heads.insert(offset, Bytes::from(head));
        }
        let mut streamed = self.directory.streamed.lock().unwrap();
        for file in streamed.values() {
            if file.suffix == self.suffix {
                let message = format!("sibling object suffix {} is already used", self.suffix);
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, message));
            }
        }
        streamed.insert(self.path.clone(), StreamedFile {
            suffix: self.suffix.clone(),
            len: self.written,
            tail,
            heads,
        });
        Ok(())
    }
}

/// Read handle for a streamed file, used only while recording the footer. It never reads the
/// sibling object: a request outside the tail and section heads fails so that a change in
/// Tantivy's open-time read pattern stays visible.
struct StreamedFileHandle {
    path: PathBuf,
    len: u64,
    tail: Buffer,
    heads: BTreeMap<u64, Bytes>,
}

impl fmt::Debug for StreamedFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamedFileHandle")
            .field("path", &self.path)
            .field("len", &self.len)
            .finish()
    }
}

impl HasLen for StreamedFileHandle {
    fn len(&self) -> usize {
        usize::try_from(self.len).unwrap_or(usize::MAX)
    }
}

impl StreamedFileHandle {
    fn read_head(&self, range: &Range<usize>) -> Option<Bytes> {
        let head = self.heads.get(&(range.start as u64))?;
        if range.len() <= head.len() {
            return Some(head.slice(..range.len()));
        }
        None
    }
}

impl FileHandle for StreamedFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end > self.len() {
            let message = format!(
                "read {range:?} past the end of streamed file {}",
                self.path.display()
            );
            return Err(io::Error::new(io::ErrorKind::InvalidInput, message));
        }
        let tail_start = self.len() - self.tail.len();
        if range.start >= tail_start {
            let bytes = self
                .tail
                .slice(range.start - tail_start..range.end - tail_start)
                .to_bytes();
            return Ok(OwnedBytes::new(bytes.to_vec()));
        }
        match self.read_head(&range) {
            Some(bytes) => Ok(OwnedBytes::new(bytes.to_vec())),
            None => {
                let message = format!(
                    "read {range:?} of streamed file {} is outside the retained tail and section heads",
                    self.path.display()
                );
                Err(io::Error::new(io::ErrorKind::Unsupported, message))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use databend_common_base::runtime::GlobalIORuntime;
    use opendal::services::Memory;

    use super::*;

    fn directory(threshold: usize) -> (Operator, InvertedIndexOutputDirectory) {
        crate::init_test_runtime();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let directory = InvertedIndexOutputDirectory::with_stream_threshold(
            operator.clone(),
            "dir/h1.index".into(),
            threshold,
        );
        (operator, directory)
    }

    fn read_object(operator: &Operator, path: &str) -> Vec<u8> {
        GlobalIORuntime::instance()
            .block_on(async { Ok(operator.read(path).await?.to_vec()) })
            .unwrap()
    }

    fn write_in_pieces(
        directory: &InvertedIndexOutputDirectory,
        path: &str,
        data: &[u8],
        piece: usize,
    ) {
        let mut write = directory.open_write(Path::new(path)).unwrap();
        for chunk in data.chunks(piece) {
            write.write_all(chunk).unwrap();
        }
        write.terminate().unwrap();
    }

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn test_small_postings_stay_inline() {
        let (operator, directory) = directory(64);
        write_in_pieces(&directory, "seg.idx", &pattern(64), 7);
        assert_eq!(
            directory
                .ram_directory()
                .atomic_read(Path::new("seg.idx"))
                .unwrap(),
            pattern(64)
        );
        assert!(directory.external_files().is_empty());
        let exists = GlobalIORuntime::instance()
            .block_on(async { Ok(operator.exists("dir/h1.index.idx").await?) })
            .unwrap();
        assert!(!exists);
    }

    #[test]
    fn test_large_postings_stream_and_keep_tail() {
        let (operator, directory) = directory(64);
        let data = pattern(1000);
        write_in_pieces(&directory, "seg.idx", &data, 13);
        write_in_pieces(&directory, "seg.pos", &data[..200], 200);
        assert_eq!(read_object(&operator, "dir/h1.index.idx"), data);
        assert_eq!(read_object(&operator, "dir/h1.index.pos"), &data[..200]);
        let external = directory.external_files();
        assert_eq!(external[Path::new("seg.idx")], ExternalFile {
            suffix: ".idx".into(),
            len: 1000
        });
        assert_eq!(external[Path::new("seg.pos")], ExternalFile {
            suffix: ".pos".into(),
            len: 200
        });
        assert!(
            !directory
                .ram_directory()
                .exists(Path::new("seg.idx"))
                .unwrap()
        );
        assert!(directory.exists(Path::new("seg.idx")).unwrap());

        let handle = directory.get_file_handle(Path::new("seg.idx")).unwrap();
        assert_eq!(handle.len(), 1000);
        assert_eq!(
            handle.read_bytes(936..1000).unwrap().as_slice(),
            &data[936..]
        );
        assert_eq!(
            handle.read_bytes(990..996).unwrap().as_slice(),
            &data[990..996]
        );
        assert_eq!(handle.read_bytes(0..8).unwrap().as_slice(), &data[0..8]);
        let error = handle.read_bytes(930..940).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(handle.read_bytes(996..1001).is_err(), "past the end");
    }

    #[test]
    fn test_tail_is_exact_at_every_flush_alignment() {
        // The retained tail must not depend on where the writes or the file end fall.
        for len in [64, 65, 127, 128, 129, 191, 192, 193, 1000] {
            for piece in [1, 3, 64, 100] {
                let (_, directory) = directory(64);
                let data = pattern(len);
                write_in_pieces(&directory, "seg.idx", &data, piece);
                if len <= 64 {
                    assert!(
                        directory.external_files().is_empty(),
                        "len={len} piece={piece}"
                    );
                    continue;
                }
                let handle = directory.get_file_handle(Path::new("seg.idx")).unwrap();
                let tail = handle.read_bytes(len - 64..len).unwrap();
                assert_eq!(
                    tail.as_slice(),
                    &data[len - 64..],
                    "len={len} piece={piece}"
                );
            }
        }
    }

    #[test]
    fn test_other_components_use_ram_directory() {
        let (_, directory) = directory(8);
        write_in_pieces(&directory, "seg.term", &pattern(1000), 100);
        assert_eq!(
            directory
                .ram_directory()
                .atomic_read(Path::new("seg.term"))
                .unwrap(),
            pattern(1000)
        );
        assert!(directory.external_files().is_empty());
        directory
            .atomic_write(Path::new("meta.json"), b"{}")
            .unwrap();
        assert_eq!(
            directory.atomic_read(Path::new("meta.json")).unwrap(),
            b"{}"
        );
    }

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) as usize
        }
    }

    fn streamed_handle(
        directory: &InvertedIndexOutputDirectory,
        path: &str,
    ) -> Arc<dyn FileHandle> {
        directory.get_file_handle(Path::new(path)).unwrap()
    }

    #[test]
    fn test_tail_offsets_map_to_the_logical_file() {
        const CAP: usize = 64;
        let (_, directory) = directory(CAP);
        let data = pattern(1000);
        write_in_pieces(&directory, "seg.idx", &data, 17);
        let handle = streamed_handle(&directory, "seg.idx");
        let tail_start = 1000 - CAP;
        // 1000 % 64 == 40, so the ring seam is at 976.
        for range in [
            tail_start..1000,
            tail_start..tail_start + 1,
            970..980,
            975..977,
            999..1000,
        ] {
            assert_eq!(
                handle.read_bytes(range.clone()).unwrap().as_slice(),
                &data[range.clone()],
                "{range:?}"
            );
        }
        for range in [0..1, 0..8, 0..64] {
            assert_eq!(
                handle.read_bytes(range.clone()).unwrap().as_slice(),
                &data[range.clone()],
                "{range:?}"
            );
        }
        for range in [
            tail_start - 1..tail_start + 1,
            56..64,
            1..8,
            0..65,
            500..600,
            0..1000,
        ] {
            let error = handle.read_bytes(range.clone()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported, "{range:?}");
        }
        assert_eq!(handle.read_bytes(1000..1000).unwrap().len(), 0);
        assert!(handle.read_bytes(999..1001).is_err());
    }

    #[test]
    fn test_section_heads_follow_flushes() {
        const CAP: usize = 64;
        let (_, directory) = directory(CAP);
        let data = pattern(2000);
        let mut write = directory.open_write(Path::new("seg.idx")).unwrap();
        // Section starts: 0 (file start), 100, 164 (flush right after an exactly filled head),
        // 172 (flush after a partial head), 1200 (flush with nothing written in between twice).
        let sections = [0usize, 100, 164, 172, 1200];
        let mut offset = 0;
        for start in sections.iter().copied().skip(1) {
            while offset < start {
                let size = 7.min(start - offset);
                write.write_all(&data[offset..offset + size]).unwrap();
                offset += size;
            }
            write.flush().unwrap();
            if start == 1200 {
                write.flush().unwrap();
            }
        }
        write.write_all(&data[offset..]).unwrap();
        write.terminate().unwrap();

        let handle = streamed_handle(&directory, "seg.idx");
        assert_eq!(
            directory.section_head_count(Path::new("seg.idx")),
            sections.len()
        );
        for (i, start) in sections.iter().copied().enumerate() {
            let next = sections.get(i + 1).copied().unwrap_or(2000);
            let end = (start + SECTION_HEAD_RETAIN).min(next);
            assert_eq!(
                handle.read_bytes(start..start + 8).unwrap().as_slice(),
                &data[start..start + 8],
                "section {start}"
            );
            assert_eq!(
                handle.read_bytes(start..end).unwrap().as_slice(),
                &data[start..end],
                "section {start}"
            );
            if end < 2000 - CAP {
                let error = handle.read_bytes(start..end + 1).unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::Unsupported, "section {start}");
            }
            let error = handle.read_bytes(start + 1..start + 8).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported, "section {start}");
        }
        let error = handle.read_bytes(300..308).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }

    /// Returns the index and the expected `total_num_tokens` of each field.
    fn build_multi_field_index<D: Directory>(
        directory: D,
    ) -> (tantivy::Index, Vec<(tantivy::schema::Field, u64)>) {
        use tantivy::schema::IndexRecordOption;
        use tantivy::schema::Schema;
        use tantivy::schema::TantivyDocument;
        use tantivy::schema::TextFieldIndexing;
        use tantivy::schema::TextOptions;

        const DOCS: u64 = 3000;
        let mut schema_builder = Schema::builder();
        let text = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        );
        let fields = [
            schema_builder.add_text_field("a", text.clone()),
            schema_builder.add_text_field("b", text.clone()),
            schema_builder.add_text_field("c", text),
        ];
        let index = tantivy::IndexBuilder::new()
            .schema(schema_builder.build())
            .open_or_create(directory)
            .unwrap();
        let mut writer = tantivy::SingleSegmentIndexWriter::new(index.clone(), 15_000_000).unwrap();
        for i in 0..DOCS {
            let mut doc = TantivyDocument::default();
            // Field k carries k + 1 tokens per document.
            for (k, field) in fields.iter().enumerate() {
                let mut words = Vec::new();
                for t in 0..=k {
                    words.push(format!("w{}", (i * 7 + t as u64 * 1301) % 997));
                }
                doc.add_text(*field, words.join(" "));
            }
            writer.add_document(doc).unwrap();
        }
        let index = writer.finalize().unwrap();
        let mut expected = Vec::new();
        for (k, field) in fields.iter().enumerate() {
            expected.push((*field, DOCS * (k as u64 + 1)));
        }
        (index, expected)
    }

    #[test]
    fn test_tantivy_opens_every_field_from_retained_bytes() {
        // Guards the assumption behind SECTION_HEAD_RETAIN: Tantivy flushes between fields.
        let (_, directory) = directory(256);
        let (index, expected) = build_multi_field_index(directory.clone());
        let external = directory.external_files();
        assert!(
            external.contains_key(Path::new(
                index.searchable_segment_metas().unwrap()[0]
                    .relative_path(tantivy::index::SegmentComponent::Postings)
                    .as_path()
            ))
        );

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment = searcher.segment_reader(0);
        for (field, tokens) in &expected {
            let inverted = segment.inverted_index(*field).unwrap();
            assert_eq!(inverted.total_num_tokens(), *tokens, "{field:?}");
        }
        let slices = super::super::collect_index_open_slices(directory.clone()).unwrap();
        let postings = index.searchable_segment_metas().unwrap()[0]
            .relative_path(tantivy::index::SegmentComponent::Postings);
        assert!(
            slices[&postings].len() >= expected.len(),
            "{:?}",
            slices[&postings]
        );
        assert!(directory.section_head_count(&postings) > expected.len());
    }

    #[test]
    fn test_extra_flushes_only_add_section_heads() {
        let (_, directory) = directory(256);
        let (index, expected) = build_multi_field_index(FlushWrapper(
            directory.clone(),
            FlushPolicy::AfterEveryWrite,
        ));
        let postings = index.searchable_segment_metas().unwrap()[0]
            .relative_path(tantivy::index::SegmentComponent::Postings);
        let heads = directory.section_head_count(&postings);
        assert!(heads > expected.len() + 1, "heads={heads}");
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment = searcher.segment_reader(0);
        for (field, tokens) in &expected {
            let inverted = segment.inverted_index(*field).unwrap();
            assert_eq!(inverted.total_num_tokens(), *tokens, "{field:?}");
        }
        let slices = super::super::collect_index_open_slices(directory).unwrap();
        assert!(slices.contains_key(&postings));
    }

    /// Simulates upstream changes to the flush pattern SECTION_HEAD_RETAIN relies on.
    #[derive(Clone, Copy, Debug)]
    enum FlushPolicy {
        Never,
        AfterEveryWrite,
    }

    #[derive(Clone, Debug)]
    struct FlushWrapper(InvertedIndexOutputDirectory, FlushPolicy);

    struct FlushWrapperWrite(Option<WritePtr>, FlushPolicy);

    impl FlushWrapperWrite {
        fn inner(&mut self) -> &mut WritePtr {
            self.0.as_mut().expect("write used after terminate")
        }
    }

    impl io::Write for FlushWrapperWrite {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let written = self.inner().write(bytes)?;
            if let FlushPolicy::AfterEveryWrite = self.1 {
                self.inner().flush()?;
            }
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            match self.1 {
                FlushPolicy::Never => Ok(()),
                FlushPolicy::AfterEveryWrite => self.inner().flush(),
            }
        }
    }

    impl TerminatingWrite for FlushWrapperWrite {
        fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
            self.0.take().expect("terminated twice").terminate()
        }
    }

    impl Directory for FlushWrapper {
        fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
            self.0.get_file_handle(path)
        }
        fn delete(&self, path: &Path) -> Result<(), DeleteError> {
            self.0.delete(path)
        }
        fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
            self.0.exists(path)
        }
        fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
            let inner = self.0.open_write(path)?;
            Ok(BufWriter::with_capacity(
                0,
                Box::new(FlushWrapperWrite(Some(inner), self.1)),
            ))
        }
        fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
            self.0.atomic_read(path)
        }
        fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
            self.0.atomic_write(path, data)
        }
        fn sync_directory(&self) -> io::Result<()> {
            self.0.sync_directory()
        }
        fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
            self.0.watch(callback)
        }
        fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
            self.0.acquire_lock(lock)
        }
    }

    #[test]
    fn test_missing_flush_between_fields_is_detected() {
        // Negative control: without the per-field flush only the first field's head exists.
        let (_, directory) = directory(256);
        let (index, expected) =
            build_multi_field_index(FlushWrapper(directory.clone(), FlushPolicy::Never));
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment = searcher.segment_reader(0);
        assert!(
            segment.inverted_index(expected[0].0).is_ok(),
            "file start is always retained"
        );
        let error = match segment.inverted_index(expected[1].0) {
            Ok(_) => panic!("field opened without its section head"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("outside the retained tail and section heads"),
            "{error}"
        );
        assert!(super::super::collect_index_open_slices(directory).is_err());
    }

    #[test]
    fn test_ring_boundaries() {
        const CAP: usize = 64;
        // (total length, write sizes): exactly full, wrap by one byte, single oversized write,
        // oversized write in the middle of a ring, many wraps, and zero-length writes mixed in.
        let cases: [(usize, &[usize]); 7] = [
            (64, &[64]),
            (65, &[64, 1]),
            (65, &[65]),
            (300, &[10, 300]),
            (1000, &[1]),
            (129, &[0, 64, 0, 64, 0, 1, 0]),
            (200, &[70, 70, 60]),
        ];
        for (len, sizes) in cases {
            let (_, directory) = directory(CAP);
            let data = pattern(len);
            let mut write = directory.open_write(Path::new("seg.idx")).unwrap();
            let mut offset = 0;
            let mut sizes = sizes.iter().copied().cycle();
            while offset < len {
                let size = sizes.next().unwrap().min(len - offset);
                write.write_all(&data[offset..offset + size]).unwrap();
                offset += size;
            }
            write.terminate().unwrap();
            if len <= CAP {
                assert!(directory.external_files().is_empty(), "len={len}");
                assert_eq!(
                    directory
                        .ram_directory()
                        .atomic_read(Path::new("seg.idx"))
                        .unwrap(),
                    data
                );
                continue;
            }
            let handle = streamed_handle(&directory, "seg.idx");
            assert_eq!(handle.len(), len, "len={len}");
            assert_eq!(
                handle.read_bytes(len - CAP..len).unwrap().as_slice(),
                &data[len - CAP..],
                "len={len}"
            );
        }
    }

    #[test]
    fn test_ring_matches_reference_for_random_write_patterns() {
        const CAP: usize = 128;
        let mut lcg = Lcg(7);
        for _ in 0..40 {
            let len = CAP + lcg.next() % (6 * CAP);
            let (_, directory) = directory(CAP);
            let data = pattern(len);
            let mut write = directory.open_write(Path::new("seg.pos")).unwrap();
            let mut offset = 0;
            while offset < len {
                let size = (lcg.next() % (CAP + CAP / 2)).min(len - offset);
                write.write_all(&data[offset..offset + size]).unwrap();
                offset += size;
            }
            write.terminate().unwrap();
            if len == CAP {
                continue;
            }
            let handle = streamed_handle(&directory, "seg.pos");
            let start = len - CAP + lcg.next() % CAP;
            let end = start + lcg.next() % (len - start + 1);
            assert_eq!(
                handle.read_bytes(start..end).unwrap().as_slice(),
                &data[start..end],
                "len={len} {start}..{end}"
            );
        }
    }

    #[test]
    fn test_empty_postings_file_stays_inline() {
        let (_, directory) = directory(64);
        let write = directory.open_write(Path::new("seg.idx")).unwrap();
        write.terminate().unwrap();
        assert!(directory.external_files().is_empty());
        assert_eq!(
            directory
                .ram_directory()
                .atomic_read(Path::new("seg.idx"))
                .unwrap(),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn test_two_streamed_files_with_one_extension_are_rejected() {
        let (_, directory) = directory(8);
        write_in_pieces(&directory, "a.idx", &pattern(100), 100);
        let mut write = directory.open_write(Path::new("b.idx")).unwrap();
        write.write_all(&pattern(100)).unwrap();
        let error = write.terminate().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }
}
