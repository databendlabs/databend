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
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;
use tantivy::directory::error::OpenReadError;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::Type;
use tantivy_common::json_path_writer::JSON_END_OF_PATH;

use super::sequential_file::SEQUENTIAL_WINDOW_SIZE;
use super::sequential_file::SequentialFileHandle;
use super::sequential_file::SequentialReadStats;
use crate::inverted_index::bundle::INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE;
use crate::inverted_index::bundle::InvertedIndexBundleFooter;
use crate::inverted_index::bundle::MANAGED_JSON_PATH;
use crate::inverted_index::bundle::META_JSON_PATH;
use crate::inverted_index::directory::FooterDirectory;
use crate::inverted_index::read_only_directory;

/// Components a merge walks front to back; everything else is read whole.
const SEQUENTIAL_EXTENSIONS: [&str; 3] = ["term", "idx", "pos"];

/// One source bundle of a merge: its footer plus read handles over the bundle object and its
/// sibling objects. Term dictionaries, postings and positions are served through
/// [`SequentialFileHandle`]s; small or randomly accessed components (`.fieldnorm`, `.fast`)
/// are read whole on first use.
#[derive(Clone)]
pub struct MergeSourceDirectory {
    inner: Arc<MergeSourceInner>,
}

struct MergeSourceInner {
    operator: Operator,
    location: String,
    footer: InvertedIndexBundleFooter,
    window_size: u64,
    handles: Mutex<HashMap<PathBuf, Arc<dyn FileHandle>>>,
    sequential_stats: Mutex<HashMap<PathBuf, Arc<SequentialReadStats>>>,
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
        Self::open_with_window_size(operator, location, bundle_size, SEQUENTIAL_WINDOW_SIZE)
    }

    pub fn open_with_window_size(
        operator: Operator,
        location: String,
        bundle_size: u64,
        window_size: u64,
    ) -> tantivy::Result<Self> {
        let footer = read_footer(&operator, &location, bundle_size)
            .map_err(|error| tantivy::TantivyError::IoError(Arc::new(error)))?;
        Ok(Self {
            inner: Arc::new(MergeSourceInner {
                operator,
                location,
                footer,
                window_size,
                handles: Mutex::new(HashMap::new()),
                sequential_stats: Mutex::new(HashMap::new()),
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

    /// Read counters of a sequentially served component, once it has been opened.
    pub fn sequential_stats(&self, path: &Path) -> Option<Arc<SequentialReadStats>> {
        let stats = self.inner.sequential_stats.lock().unwrap();
        stats.get(path).cloned()
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

    fn is_sequential(path: &Path) -> bool {
        match path.extension().and_then(|extension| extension.to_str()) {
            Some(extension) => SEQUENTIAL_EXTENSIONS.contains(&extension),
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

    fn open_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let (object, range) = self
            .locate(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        let len = range.end - range.start;
        if Self::is_sequential(path) {
            let handle = SequentialFileHandle::new(
                self.inner.operator.clone(),
                object,
                path.to_path_buf(),
                range.start,
                len,
                self.inner.window_size,
            );
            let mut stats = self.inner.sequential_stats.lock().unwrap();
            stats.insert(path.to_path_buf(), handle.stats());
            return Ok(Arc::new(handle));
        }
        let bytes = read_range(&self.inner.operator, &object, range)
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
        Ok(Arc::new(OwnedBytes::new(bytes)))
    }
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

/// Reads `range` of `object` on the calling thread.
fn read_range(operator: &Operator, object: &str, range: Range<u64>) -> io::Result<Vec<u8>> {
    if range.is_empty() {
        return Ok(Vec::new());
    }
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
/// footer is larger than the initial tail.
fn read_footer(
    operator: &Operator,
    location: &str,
    bundle_size: u64,
) -> io::Result<InvertedIndexBundleFooter> {
    let initial = INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE as u64;
    let tail_start = bundle_size.saturating_sub(initial);
    let tail = read_range(operator, location, tail_start..bundle_size)?;
    let footer_start =
        InvertedIndexBundleFooter::footer_start_from_tail(&tail, bundle_size, tail_start)?;
    if footer_start >= tail_start {
        return InvertedIndexBundleFooter::parse_footer_from_tail(&tail, bundle_size, tail_start);
    }
    let tail = read_range(operator, location, footer_start..bundle_size)?;
    InvertedIndexBundleFooter::parse_footer_from_tail(&tail, bundle_size, footer_start)
}

#[cfg(test)]
mod tests {
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
        let schema = index.schema();
        let title = schema.get_field("title").unwrap();
        let meta = schema.get_field("meta").unwrap();
        let mut writer = SingleSegmentIndexWriter::new(index, 16 * 1024 * 1024).unwrap();
        for i in 0..ROWS {
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
        init_test_runtime();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let directory = InvertedIndexOutputDirectory::with_stream_threshold(
            operator.clone(),
            "t/h1.index".into(),
            threshold,
        );
        let index = add_documents(Index::create(directory.clone(), schema(), settings()).unwrap());
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
    fn test_source_reads_every_component_forward() {
        const WINDOW: u64 = 64 * 1024;
        let (operator, bundle_size) = write_bundle(4096);
        let source = MergeSourceDirectory::open_with_window_size(
            operator,
            "t/h1.index".into(),
            bundle_size,
            WINDOW,
        )
        .unwrap();
        let index = source.open_index().unwrap();
        let walked = walk(&index);

        let reference =
            add_documents(Index::create(RamDirectory::default(), schema(), settings()).unwrap());
        assert_eq!(walked, walk(&reference));
        assert!(walked.len() > 100);

        let segment = index.searchable_segment_metas().unwrap().remove(0);
        for component in [
            SegmentComponent::Terms,
            SegmentComponent::Postings,
            SegmentComponent::Positions,
        ] {
            let path = segment.relative_path(component);
            let len = source.footer().file_len(&path).unwrap();
            let stats = source.sequential_stats(&path).unwrap();
            assert_eq!(stats.backward_reads(), 0, "{path:?}");
            assert!(
                stats.fetched_bytes() <= len + stats.fetches() * WINDOW / 4,
                "{path:?}"
            );
            assert!(
                stats.fetches() <= len / WINDOW + 2,
                "{path:?} fetches={}",
                stats.fetches()
            );
        }
        assert!(
            source
                .sequential_stats(&segment.relative_path(SegmentComponent::FastFields))
                .is_none()
        );
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
