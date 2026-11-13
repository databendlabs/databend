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

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use databend_storages_common_io::OperatorRangeReader;
use databend_storages_common_io::RangeReader;
use opendal::Operator;
use tantivy::Directory;
use tantivy::Index;
use tantivy::SegmentReader;
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;
use tantivy::directory::error::OpenReadError;
use tantivy::schema::FieldType;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::Schema;
use tantivy::schema::Type;
use tantivy_common::json_path_writer::JSON_END_OF_PATH;

use super::invalid;
use super::streaming_file::STREAMING_CHUNK_SIZE;
use super::streaming_file::StreamingFileHandle;
use super::streaming_file::StreamingReadStats;
use crate::inverted_index::bundle::InvertedIndexBundleFooter;
use crate::inverted_index::bundle::MANAGED_JSON_PATH;
use crate::inverted_index::bundle::META_JSON_PATH;
use crate::inverted_index::directory::FooterDirectory;
use crate::inverted_index::read_only_directory;

/// Components the window mergers stream front to back; everything else, including the term
/// dictionary every window and the partitioner consult, is read whole.
const STREAMED_EXTENSIONS: [&str; 2] = ["idx", "pos"];

/// Initial tail read when opening a source: large enough that the footer, the small components
/// and the term dictionary of a typical block bundle arrive in one request.
pub const SOURCE_OPEN_TAIL_READ_SIZE: u64 = 4 * 1024 * 1024;

/// One source of a merge, as the block metadata describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeSource {
    pub location: String,
    pub bundle_size: u64,
    pub num_rows: u32,
}

/// One opened source: its directory, index and single segment.
#[derive(Clone)]
pub struct MergeSourceReader {
    pub directory: MergeSourceDirectory,
    pub index: Index,
    pub reader: SegmentReader,
}

/// Opens `source` and checks it is what the merge expects: one segment without deletes holding
/// `num_rows` docs, with the schema of the current index definition.
pub fn open_merge_source(
    operator: Operator,
    source: &MergeSource,
    expected_schema: Option<&Schema>,
) -> tantivy::Result<MergeSourceReader> {
    let directory =
        MergeSourceDirectory::open(operator, source.location.clone(), source.bundle_size)?;
    let index = directory.open_index()?;
    let searcher = index.reader()?.searcher();
    if searcher.segment_readers().len() != 1 {
        return Err(invalid(format!(
            "{} has {} segments, expected one",
            source.location,
            searcher.segment_readers().len()
        ))
        .into());
    }
    let reader = searcher.segment_reader(0).clone();
    if reader.has_deletes() {
        return Err(invalid("source index has deleted docs").into());
    }
    if reader.max_doc() != source.num_rows {
        return Err(invalid(format!(
            "{} has {} rows, expected {}",
            source.location,
            reader.max_doc(),
            source.num_rows
        ))
        .into());
    }
    let schema = index.schema();
    check_schema(&schema)?;
    if expected_schema.is_some_and(|expected| *expected != schema) {
        return Err(invalid(format!(
            "{} has a schema that differs from the current index definition",
            source.location
        ))
        .into());
    }
    Ok(MergeSourceReader {
        directory,
        index,
        reader,
    })
}

/// The merge reproduces what Databend's index writer produces: indexed text and JSON fields,
/// fast fields only on JSON, nothing stored.
pub fn check_schema(schema: &Schema) -> io::Result<()> {
    for (_, entry) in schema.fields() {
        if entry.is_stored() {
            return Err(invalid(format!("field {} is stored", entry.name())));
        }
        if entry.is_fast() && !matches!(entry.field_type(), FieldType::JsonObject(_)) {
            return Err(invalid(format!(
                "field {} is a non-JSON fast field",
                entry.name()
            )));
        }
        if !matches!(
            entry.field_type(),
            FieldType::Str(_) | FieldType::JsonObject(_)
        ) {
            return Err(invalid(format!(
                "field {} is neither text nor JSON",
                entry.name()
            )));
        }
    }
    Ok(())
}

/// One source bundle of a merge: its footer plus read handles over the bundle object and its
/// sibling objects. Postings and positions are served through shared [`StreamingFileHandle`]s;
/// the term dictionary and the small components (`.fieldnorm`, `.fast`, `.store`) are read
/// whole when the source is opened.
#[derive(Clone)]
pub struct MergeSourceDirectory {
    inner: Arc<MergeSourceInner>,
}

struct MergeSourceInner {
    operator: Operator,
    location: String,
    footer: InvertedIndexBundleFooter,
    chunk_size: u64,
    handles: Mutex<HashMap<PathBuf, Arc<dyn FileHandle>>>,
    streams: Mutex<HashMap<PathBuf, Arc<StreamingFileHandle>>>,
}

impl fmt::Debug for MergeSourceDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("MergeSourceDirectory")
            .field("location", &self.inner.location)
            .finish_non_exhaustive()
    }
}

impl MergeSourceDirectory {
    /// Reads the footer of the bundle at `location` (bounded tail reads, as the query path does)
    /// and returns a read-only directory over it.
    pub fn open(operator: Operator, location: String, bundle_size: u64) -> tantivy::Result<Self> {
        Self::open_with_chunk_size(operator, location, bundle_size, STREAMING_CHUNK_SIZE)
    }

    pub fn open_with_chunk_size(
        operator: Operator,
        location: String,
        bundle_size: u64,
        chunk_size: u64,
    ) -> tantivy::Result<Self> {
        Self::open_with(
            operator,
            location,
            bundle_size,
            chunk_size,
            SOURCE_OPEN_TAIL_READ_SIZE,
        )
    }

    fn open_with(
        operator: Operator,
        location: String,
        bundle_size: u64,
        chunk_size: u64,
        initial_tail: u64,
    ) -> tantivy::Result<Self> {
        let (footer, tail) = read_footer(&operator, &location, bundle_size, initial_tail)
            .map_err(|error| tantivy::TantivyError::IoError(Arc::new(error)))?;
        let handles = read_whole_files(&operator, &location, &footer, &tail)
            .map_err(|error| tantivy::TantivyError::IoError(Arc::new(error)))?;
        Ok(Self {
            inner: Arc::new(MergeSourceInner {
                operator,
                location,
                footer,
                chunk_size,
                handles: Mutex::new(handles),
                streams: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Opens the index. Reads Tantivy issues while opening come from the footer's open slices;
    /// everything after that goes through this directory.
    pub fn open_index(&self) -> tantivy::Result<Index> {
        Index::open(FooterDirectory::new(
            self.clone(),
            self.inner.footer.clone(),
        ))
    }

    pub fn footer(&self) -> &InvertedIndexBundleFooter {
        &self.inner.footer
    }

    /// Read counters of a streamed component, once it has been opened.
    pub fn streaming_stats(&self, path: &Path) -> Option<Arc<StreamingReadStats>> {
        let streams = self.inner.streams.lock().unwrap();
        streams.get(path).map(|stream| stream.stats())
    }

    /// Object and byte range holding a segment file: inline in the bundle or a sibling object.
    fn locate(&self, path: &Path) -> Option<(String, Range<u64>)> {
        let footer = &self.inner.footer;
        if let Some(range) = footer.file_ranges.get(path) {
            return Some((self.inner.location.clone(), range));
        }
        let external = footer.external_files.get(path)?;
        let object = format!("{}{}", self.inner.location, external.suffix);
        Some((object, 0..external.len))
    }

    fn is_streamed(path: &Path) -> bool {
        match path.extension().and_then(|extension| extension.to_str()) {
            Some(extension) => STREAMED_EXTENSIONS.contains(&extension),
            None => false,
        }
    }

    fn inline_file(&self, path: &Path) -> Option<OwnedBytes> {
        if path == Path::new(MANAGED_JSON_PATH) {
            return Some(OwnedBytes::new(self.inner.footer.managed_json.clone()));
        }
        if path == Path::new(META_JSON_PATH) {
            return Some(OwnedBytes::new(self.inner.footer.meta_json.clone()));
        }
        None
    }

    /// Inline whole-read files are all in `handles` since `open`; only streamed files and
    /// (unexpected) external whole-read files are opened here.
    fn open_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let (object, range) = self
            .locate(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        let len = range.end - range.start;
        if Self::is_streamed(path) {
            let handle = Arc::new(StreamingFileHandle::new(
                self.inner.operator.clone(),
                object,
                path.to_path_buf(),
                range.start,
                len,
                self.inner.chunk_size,
            ));
            let mut streams = self.inner.streams.lock().unwrap();
            streams.insert(path.to_path_buf(), handle.clone());
            return Ok(handle);
        }
        let bytes = read_range(&self.inner.operator, &object, range)
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
        Ok(Arc::new(OwnedBytes::new(bytes)))
    }
}

/// Bytes of one object range already in memory.
struct ObjectTail {
    start: u64,
    bytes: Vec<u8>,
}

impl ObjectTail {
    fn slice(&self, range: &Range<u64>) -> Option<&[u8]> {
        if range.start < self.start || range.end > self.start + self.bytes.len() as u64 {
            return None;
        }
        let start = (range.start - self.start) as usize;
        Some(&self.bytes[start..start + (range.end - range.start) as usize])
    }
}

/// Handles for every inline file that is read whole. The bundle stores them next to each
/// other ahead of `.term` and the footer, so whatever the tail read did not already cover is
/// fetched with one request instead of one per file.
fn read_whole_files(
    operator: &Operator,
    location: &str,
    footer: &InvertedIndexBundleFooter,
    tail: &ObjectTail,
) -> io::Result<HashMap<PathBuf, Arc<dyn FileHandle>>> {
    let mut handles: HashMap<PathBuf, Arc<dyn FileHandle>> = HashMap::new();
    let mut missing: Vec<(&PathBuf, Range<u64>)> = Vec::new();
    let mut union: Option<Range<u64>> = None;
    for (path, range) in &footer.file_ranges.files {
        if MergeSourceDirectory::is_streamed(path) {
            continue;
        }
        if let Some(bytes) = tail.slice(range) {
            handles.insert(path.clone(), Arc::new(OwnedBytes::new(bytes.to_vec())));
            continue;
        }
        union = Some(match union {
            None => range.clone(),
            Some(union) => union.start.min(range.start)..union.end.max(range.end),
        });
        missing.push((path, range.clone()));
    }
    let Some(union) = union else {
        return Ok(handles);
    };
    let fetched = ObjectTail {
        start: union.start,
        bytes: read_range(operator, location, union)?,
    };
    for (path, range) in missing {
        let bytes = fetched.slice(&range).ok_or_else(|| {
            io::Error::other(format!("{} outside the fetched range", path.display()))
        })?;
        handles.insert(path.clone(), Arc::new(OwnedBytes::new(bytes.to_vec())));
    }
    Ok(handles)
}

impl Directory for MergeSourceDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        if let Some(bytes) = self.inline_file(path) {
            return Ok(Arc::new(bytes));
        }
        let mut handles = self.inner.handles.lock().unwrap();
        if let Some(handle) = handles.get(path) {
            return Ok(Arc::clone(handle));
        }
        let handle = self.open_handle(path)?;
        handles.insert(path.to_path_buf(), Arc::clone(&handle));
        Ok(handle)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let handle = self.get_file_handle(path)?;
        handle
            .read_bytes(0..handle.len())
            .map(|bytes| bytes.as_slice().to_vec())
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.inline_file(path).is_some() || self.inner.footer.file_len(path).is_some())
    }

    read_only_directory!();
}

/// What the posting list behind a JSON field dictionary key actually encodes. The field option
/// alone cannot tell: JSON number, bool and date terms carry neither frequencies nor positions
/// even when the field records both. `key` is the dictionary key, i.e. the term without field
/// id and type tag.
pub fn json_term_record_option(field_option: IndexRecordOption, key: &[u8]) -> IndexRecordOption {
    if !field_option.has_freq() {
        return field_option;
    }
    // JSON key: path, JSON_END_OF_PATH, value type code, value.
    let Some(end_of_path) = key.iter().position(|byte| *byte == JSON_END_OF_PATH) else {
        return IndexRecordOption::Basic;
    };
    let Some(code) = key.get(end_of_path + 1) else {
        return IndexRecordOption::Basic;
    };
    match Type::from_code(*code) {
        Some(Type::Str) => field_option,
        _ => IndexRecordOption::Basic,
    }
}

// Requests issued by `read_range` on this thread, so tests can pin how many an open needs.
#[cfg(test)]
thread_local! {
    pub(crate) static WHOLE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Reads `range` of `object` on the calling thread.
fn read_range(operator: &Operator, object: &str, range: Range<u64>) -> io::Result<Vec<u8>> {
    if range.is_empty() {
        return Ok(Vec::new());
    }
    #[cfg(test)]
    WHOLE_READS.with(|reads| reads.set(reads.get() + 1));
    let mut reader = OperatorRangeReader::new(operator.clone(), object.to_string(), 1);
    let data = RangeReader::read(&mut reader, range.clone())
        .map_err(|error| io::Error::other(error.to_string()))?;
    if data.len() as u64 != range.end - range.start {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("{object} returned {} bytes for {range:?}", data.len()),
        ));
    }
    Ok(data.to_vec())
}

/// Footer of the bundle at `location`: one bounded tail read, plus an exact read when the
/// footer is larger than the initial tail. Returns the tail bytes too; they often already hold
/// the small components stored ahead of the footer.
fn read_footer(
    operator: &Operator,
    location: &str,
    bundle_size: u64,
    initial_tail: u64,
) -> io::Result<(InvertedIndexBundleFooter, ObjectTail)> {
    let tail_start = bundle_size.saturating_sub(initial_tail);
    let tail = read_range(operator, location, tail_start..bundle_size)?;
    let footer_start =
        InvertedIndexBundleFooter::footer_start_from_tail(&tail, bundle_size, tail_start)?;
    if footer_start >= tail_start {
        let footer =
            InvertedIndexBundleFooter::parse_footer_from_tail(&tail, bundle_size, tail_start)?;
        return Ok((footer, ObjectTail {
            start: tail_start,
            bytes: tail,
        }));
    }
    let tail = read_range(operator, location, footer_start..bundle_size)?;
    let footer =
        InvertedIndexBundleFooter::parse_footer_from_tail(&tail, bundle_size, footer_start)?;
    Ok((footer, ObjectTail {
        start: footer_start,
        bytes: tail,
    }))
}

#[cfg(test)]
pub(super) mod tests {
    use databend_common_base::runtime::GlobalIORuntime;
    use opendal::services::Memory;
    use tantivy::IndexSettings;
    use tantivy::SingleSegmentIndexWriter;
    use tantivy::directory::RamDirectory;
    use tantivy::index::SegmentComponent;
    use tantivy::schema::IndexRecordOption;
    use tantivy::schema::JsonObjectOptions;
    use tantivy::schema::OwnedValue;
    use tantivy::schema::Schema;
    use tantivy::schema::TantivyDocument;
    use tantivy::schema::TextFieldIndexing;
    use tantivy::schema::TextOptions;

    use super::*;
    use crate::init_test_runtime;
    use crate::inverted_index::InvertedIndexBundleBuilder;
    use crate::inverted_index::InvertedIndexOutputDirectory;
    use crate::inverted_index::merge::test_util::walk;

    const ROWS: usize = 3000;

    fn schema() -> Schema {
        let indexing = TextFieldIndexing::default()
            .set_tokenizer("default")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let mut builder = Schema::builder();
        builder.add_text_field(
            "title",
            TextOptions::default().set_indexing_options(indexing.clone()),
        );
        builder.add_json_field(
            "meta",
            JsonObjectOptions::default()
                .set_indexing_options(indexing)
                .set_fast(Some("raw")),
        );
        builder.build()
    }

    fn add_documents(index: Index) -> Index {
        add_rows(index, ROWS)
    }

    fn add_rows(index: Index, rows: usize) -> Index {
        let schema = index.schema();
        let title = schema.get_field("title").unwrap();
        let meta = schema.get_field("meta").unwrap();
        let mut writer = SingleSegmentIndexWriter::new(index, 16 * 1024 * 1024).unwrap();
        for i in 0..rows {
            let mut doc = TantivyDocument::new();
            doc.add_text(title, format!("alpha beta w{} delta w{}", i % 97, i));
            let json = serde_json::json!({ "tag": format!("t{}", i % 5), "n": i });
            doc.add_field_value(meta, &OwnedValue::from(json));
            writer.add_document(doc).unwrap();
        }
        writer.finalize().unwrap()
    }

    fn settings() -> IndexSettings {
        IndexSettings {
            docstore_compress_dedicated_thread: false,
            ..Default::default()
        }
    }

    fn write_bundle(threshold: usize) -> (Operator, u64) {
        write_bundle_with_rows(threshold, ROWS)
    }

    pub(super) fn write_bundle_with_rows(threshold: usize, rows: usize) -> (Operator, u64) {
        init_test_runtime();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let directory = InvertedIndexOutputDirectory::with_stream_threshold(
            operator.clone(),
            "t/h1.index".into(),
            threshold,
        );
        let index = add_rows(
            Index::create(directory.clone(), schema(), settings()).unwrap(),
            rows,
        );
        let builder = InvertedIndexBundleBuilder::try_create(
            directory.clone(),
            index,
            directory.external_files(),
        )
        .unwrap();
        let mut bundle = Vec::new();
        let sizes = builder.write_to(&mut bundle).unwrap();
        let writer = operator.clone();
        GlobalIORuntime::instance()
            .block_on(async move { Ok(writer.write("t/h1.index", bundle).await?) })
            .unwrap();
        (operator, sizes.bundle)
    }

    #[test]
    fn test_source_streams_postings_and_positions_forward() {
        const CHUNK: u64 = 64 * 1024;
        let (operator, bundle_size) = write_bundle(4096);
        let source = MergeSourceDirectory::open_with_chunk_size(
            operator,
            "t/h1.index".into(),
            bundle_size,
            CHUNK,
        )
        .unwrap();
        let index = source.open_index().unwrap();
        let walked = walk(&index);

        let reference =
            add_documents(Index::create(RamDirectory::default(), schema(), settings()).unwrap());
        assert_eq!(walked, walk(&reference));
        assert!(walked.len() > 100);

        let segment = index.searchable_segment_metas().unwrap().remove(0);
        for component in [SegmentComponent::Postings, SegmentComponent::Positions] {
            let path = segment.relative_path(component);
            let len = source.footer().file_len(&path).unwrap();
            let stats = source.streaming_stats(&path).unwrap();
            assert_eq!(stats.behind_reads(), 0, "{path:?}");
            assert_eq!(stats.fetched_bytes(), len, "{path:?}");
            assert_eq!(stats.fetches(), len.div_ceil(CHUNK), "{path:?}");
        }
        for component in [SegmentComponent::Terms, SegmentComponent::FastFields] {
            assert!(
                source
                    .streaming_stats(&segment.relative_path(component))
                    .is_none()
            );
        }
    }

    #[test]
    fn test_source_with_inline_components_reads_the_same() {
        let (operator, bundle_size) = write_bundle(usize::MAX);
        let source =
            MergeSourceDirectory::open(operator, "t/h1.index".into(), bundle_size).unwrap();
        assert!(
            source
                .footer()
                .external_files
                .get(Path::new("x.idx"))
                .is_none()
        );
        let index = source.open_index().unwrap();
        let reference =
            add_documents(Index::create(RamDirectory::default(), schema(), settings()).unwrap());
        assert_eq!(walk(&index), walk(&reference));
    }

    #[test]
    fn test_missing_bundle_is_an_error() {
        init_test_runtime();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        assert!(MergeSourceDirectory::open(operator, "t/none.index".into(), 4096).is_err());
    }
}

#[cfg(test)]
mod record_option_tests {
    use tantivy::schema::IndexRecordOption;
    use tantivy::schema::Type;
    use tantivy_common::json_path_writer::JSON_END_OF_PATH;

    use super::json_term_record_option;

    fn json_key(path: &str, value_type: Type) -> Vec<u8> {
        let mut key = path.as_bytes().to_vec();
        key.push(JSON_END_OF_PATH);
        key.push(value_type.to_code());
        key.extend_from_slice(b"value");
        key
    }

    #[test]
    fn test_json_term_record_option_follows_the_value_type() {
        for option in [
            IndexRecordOption::Basic,
            IndexRecordOption::WithFreqs,
            IndexRecordOption::WithFreqsAndPositions,
        ] {
            assert_eq!(
                json_term_record_option(option, &json_key("tag", Type::Str)),
                option
            );
            for value_type in [Type::I64, Type::U64, Type::F64, Type::Bool, Type::Date] {
                assert_eq!(
                    json_term_record_option(option, &json_key("n", value_type)),
                    IndexRecordOption::Basic,
                    "{option:?} {value_type:?}"
                );
            }
            assert_eq!(
                json_term_record_option(option, b"no-end-of-path"),
                IndexRecordOption::Basic
            );
        }
    }
}

#[cfg(test)]
mod open_request_tests {
    use super::tests::write_bundle_with_rows;
    use super::*;

    fn requests<T>(work: impl FnOnce() -> T) -> (T, u64) {
        let before = WHOLE_READS.with(|reads| reads.get());
        let value = work();
        (value, WHOLE_READS.with(|reads| reads.get()) - before)
    }

    #[test]
    fn test_small_bundle_opens_with_the_tail_read_only() {
        let (operator, bundle_size) = write_bundle_with_rows(usize::MAX, 200);
        assert!(bundle_size < SOURCE_OPEN_TAIL_READ_SIZE);
        let (source, requests) = requests(|| {
            MergeSourceDirectory::open(operator, "t/h1.index".into(), bundle_size).unwrap()
        });
        assert_eq!(
            requests, 1,
            "footer and every whole-read file come from the tail"
        );
        let handles = source.inner.handles.lock().unwrap();
        assert_eq!(
            handles.len(),
            4,
            "store, fast, fieldnorm and term: {:?}",
            handles.keys()
        );
    }

    #[test]
    fn test_files_outside_the_tail_are_fetched_in_one_request() {
        // A 4 KiB initial tail: the footer needs an exact second read, and every whole-read
        // file lies before it.
        let (operator, bundle_size) = write_bundle_with_rows(usize::MAX, 3000);
        let (source, requests) = requests(|| {
            MergeSourceDirectory::open_with(operator, "t/h1.index".into(), bundle_size, 4096, 4096)
                .unwrap()
        });
        let footer = source.footer();
        for (path, range) in &footer.file_ranges.files {
            if !MergeSourceDirectory::is_streamed(path) {
                assert!(range.end <= footer.footer_start, "{path:?}");
            }
        }
        let tail_start = bundle_size - 4096;
        let footer_reads = if footer.footer_start >= tail_start {
            1
        } else {
            2
        };
        assert_eq!(
            requests,
            footer_reads + 1,
            "footer reads plus one for the rest"
        );
        assert_eq!(source.inner.handles.lock().unwrap().len(), 4);
    }
}
