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
use std::io;
use std::mem::size_of;
use std::path::Path;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::RwLock;
use tantivy::Index;
use tantivy::IndexReader;
use tantivy::ReloadPolicy;
use tantivy::Searcher;
use tantivy::Term;
use tantivy::directory::CompositeFile;
use tantivy::directory::FileSlice;
use tantivy::index::Segment;
use tantivy::index::SegmentComponent;
use tantivy::postings::TermInfo;
use tantivy::query::Bm25StatisticsProvider;
use tantivy::schema::Field;

use super::directory::FooterDirectory;

/// Terms whose infos one searcher remembers; it forgets them all when it holds this many.
const MAX_REMEMBERED_TERMS: usize = 4096;
/// Charged for each remembered term: the term, its info and the map entry.
const REMEMBERED_TERM_BYTES: usize = 128;

/// The postings and positions of a field in the segment of a bundle.
#[derive(Clone)]
pub struct FieldPostings {
    pub postings: FileSlice,
    /// None when the segment has no positions of the field.
    pub positions: Option<FileSlice>,
}

/// An inverted index bundle opened once, shared by the searches of its block.
///
/// It reads through a shared `SearchPinDirectory` that keeps, for all searches, the ranges of
/// the files `retains` accepts: the term dictionary, fieldnorms and fast fields. A search runs
/// in `with_search_scope`, which pins the postings and positions it warms until it completes.
/// The term infos its searches looked up are remembered, so a later search warms the postings
/// of a term and scores it without decoding its dictionary block again.
pub struct InvertedIndexSearcher {
    searcher: Searcher,
    segment: Segment,
    directory: FooterDirectory,
    term_infos: RwLock<HashMap<Term, Option<TermInfo>>>,
    field_postings: RwLock<HashMap<Field, FieldPostings>>,
    memory_size: usize,
}

impl InvertedIndexSearcher {
    /// Whether the shared pin of a searcher keeps the ranges of `path` for all its searches.
    /// Each of these files is charged in full: its pinned ranges grow up to the whole file.
    pub fn retains(path: &Path) -> bool {
        matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("term" | "fieldnorm" | "fast")
        )
    }

    /// Opens the single segment of a bundle, through a directory over a shared pin.
    pub fn open(directory: FooterDirectory) -> Result<Self> {
        let index = Index::open(directory.clone())?;
        let reader: IndexReader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        let [reader] = searcher.segment_readers() else {
            return Err(ErrorCode::StorageOther(format!(
                "inverted index bundle expects one Tantivy segment, got {}",
                searcher.segment_readers().len()
            )));
        };
        // Tantivy lazily inserts field readers under a poisonable lock. Open them from the
        // footer's index-open slices before publishing the searcher, not during a later query.
        for (field, entry) in index.schema().fields() {
            if entry.is_indexed() {
                reader.inverted_index(field)?;
            }
        }
        let segment = index
            .searchable_segments()?
            .into_iter()
            .find(|segment| segment.id() == reader.segment_id())
            .ok_or_else(|| {
                ErrorCode::Internal("inverted-index searcher segment is missing from the index")
            })?;

        let footer = directory.footer();
        let retained_bytes = footer
            .file_ranges
            .files
            .keys()
            .chain(footer.external_files.files.keys())
            .filter(|path| Self::retains(path))
            .filter_map(|path| footer.file_len(path))
            .sum::<u64>();
        let memory_size = size_of::<Self>()
            + usize::try_from(footer.footer_size + retained_bytes).unwrap_or(usize::MAX)
            + MAX_REMEMBERED_TERMS * REMEMBERED_TERM_BYTES;
        Ok(Self {
            searcher,
            segment,
            directory,
            term_infos: RwLock::new(HashMap::new()),
            field_postings: RwLock::new(HashMap::new()),
            memory_size,
        })
    }

    pub fn searcher(&self) -> &Searcher {
        &self.searcher
    }

    pub fn segment(&self) -> &Segment {
        &self.segment
    }

    /// The directory over the shared pin, which also reads whole physical components.
    pub fn directory(&self) -> &FooterDirectory {
        &self.directory
    }

    /// What its memory is charged: the most it holds once its pin keeps every retained file.
    pub fn memory_size(&self) -> usize {
        self.memory_size
    }

    /// The remembered info of `term`, `Some(None)` for a term not in the dictionary.
    ///
    /// A term is remembered after its dictionary block was read through the shared pin, which
    /// keeps the block: the synchronous lookups of a search find it pinned.
    pub fn term_info(&self, term: &Term) -> Option<Option<TermInfo>> {
        self.term_infos.read().get(term).cloned()
    }

    pub fn remember_term_info(&self, term: &Term, info: Option<TermInfo>) {
        let term = term.clone();
        let mut term_infos = self.term_infos.write();
        if term_infos.len() >= MAX_REMEMBERED_TERMS && !term_infos.contains_key(&term) {
            term_infos.clear();
        }
        term_infos.insert(term, info);
    }

    /// The slices Tantivy reads the posting lists and positions of `field` from.
    pub fn field_postings(&self, field: Field) -> io::Result<FieldPostings> {
        if let Some(postings) = self.field_postings.read().get(&field) {
            return Ok(postings.clone());
        }
        let missing = || io::Error::other(format!("inverted index has no postings for {field:?}"));
        let open = |component| -> io::Result<Option<FileSlice>> {
            let file = match self.segment.open_read(component) {
                Ok(file) => file,
                Err(_) if component == SegmentComponent::Positions => return Ok(None),
                Err(error) => return Err(io::Error::other(error)),
            };
            Ok(CompositeFile::open(&file)?.open_read(field))
        };
        // As `InvertedIndexReader::new`: the postings start after the 8-byte token count.
        let (_, postings) = open(SegmentComponent::Postings)?
            .ok_or_else(missing)?
            .split(8);
        let positions = open(SegmentComponent::Positions)?;
        let postings = FieldPostings {
            postings,
            positions,
        };
        self.field_postings.write().insert(field, postings.clone());
        Ok(postings)
    }
}

/// BM25 statistics of the searcher, with the document frequencies of remembered terms.
impl Bm25StatisticsProvider for InvertedIndexSearcher {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        self.searcher.total_num_tokens(field)
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        self.searcher.total_num_docs()
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        match self.term_info(term) {
            // One segment: the frequency in it is the frequency in the index.
            Some(info) => Ok(info.map_or(0, |info| u64::from(info.doc_freq))),
            None => self.searcher.doc_freq(term),
        }
    }
}
