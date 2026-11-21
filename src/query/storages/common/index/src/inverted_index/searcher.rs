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

/// Buckets of the table of remembered terms. It is never filled past what these buckets hold,
/// so it never grows larger: `REMEMBERED_TERMS_BYTES` bounds its memory.
const TERM_TABLE_BUCKETS: usize = 1024;
/// Terms one searcher remembers: as many as its table holds, 7 of 8 buckets.
const MAX_REMEMBERED_TERMS: usize = TERM_TABLE_BUCKETS / 8 * 7;
/// Heap bytes of the terms one searcher remembers.
const MAX_REMEMBERED_TERM_HEAP_BYTES: usize = 32 * 1024;
/// A term whose clone takes more heap bytes is looked up in the dictionary by each search.
const MAX_REMEMBERED_TERM_BYTES: usize = 128;
/// Once full, the terms it refuses before it forgets them all and starts over: a few times
/// what it holds. The terms remembered first, mostly the frequent ones, stay unless the
/// searches change.
const REFUSED_TERMS_BEFORE_RESET: usize = 4 * MAX_REMEMBERED_TERMS;
/// The most the remembered terms take: the table with its control bytes, and the terms.
const REMEMBERED_TERMS_BYTES: usize = TERM_TABLE_BUCKETS
    * (size_of::<(Term, Option<TermInfo>)>() + 1)
    + 16
    + MAX_REMEMBERED_TERM_HEAP_BYTES;

/// The postings and positions of a field in the segment of a bundle.
#[derive(Clone)]
pub struct FieldPostings {
    pub postings: FileSlice,
    /// None when the segment has no positions of the field.
    pub positions: Option<FileSlice>,
}

/// The term infos a searcher remembers, within `REMEMBERED_TERMS_BYTES`.
#[derive(Default)]
struct RememberedTerms {
    infos: HashMap<Term, Option<TermInfo>>,
    /// Heap bytes of the remembered terms.
    heap_bytes: usize,
    /// Terms refused since it was last full and reset.
    refused: usize,
}

impl RememberedTerms {
    /// The heap bytes a clone of `term` takes, unless it is too long to remember: its value
    /// and type tag, rounded up to the allocator's 16-byte size classes up to 128 bytes.
    fn heap_bytes(term: &Term) -> Option<usize> {
        let bytes = term.serialized_value_bytes().len() + 1;
        (bytes <= MAX_REMEMBERED_TERM_BYTES).then(|| bytes.next_multiple_of(16))
    }

    fn get(&self, term: &Term) -> Option<Option<TermInfo>> {
        self.infos.get(term).cloned()
    }

    /// `heap_bytes` is `Self::heap_bytes(&term)`.
    fn insert(&mut self, term: Term, heap_bytes: usize, info: Option<TermInfo>) {
        if let Some(remembered) = self.infos.get_mut(&term) {
            *remembered = info;
            return;
        }
        if self.infos.len() >= MAX_REMEMBERED_TERMS
            || self.heap_bytes + heap_bytes > MAX_REMEMBERED_TERM_HEAP_BYTES
        {
            self.refused += 1;
            if self.refused < REFUSED_TERMS_BEFORE_RESET {
                return;
            }
            // Clearing keeps the table, which never grows past its buckets.
            self.infos.clear();
            self.heap_bytes = 0;
            self.refused = 0;
        }
        // An allocation failure while the table grows unwinds before it changes it.
        self.infos.insert(term, info);
        self.heap_bytes += heap_bytes;
    }
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
    term_infos: RwLock<RememberedTerms>,
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

        // Saturating: the lengths come from the footer of the object, which may be corrupt.
        let footer = directory.footer();
        let retained_bytes = footer
            .file_ranges
            .files
            .keys()
            .chain(footer.external_files.files.keys())
            .filter(|path| Self::retains(path))
            .filter_map(|path| footer.file_len(path))
            .fold(footer.footer_size, u64::saturating_add);
        let memory_size = usize::try_from(retained_bytes)
            .unwrap_or(usize::MAX)
            .saturating_add(size_of::<Self>() + REMEMBERED_TERMS_BYTES);
        Ok(Self {
            searcher,
            segment,
            directory,
            term_infos: RwLock::default(),
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

    /// What its memory is charged: the most it holds, once its pin keeps every retained file
    /// and it remembers as many terms as it can.
    pub fn memory_size(&self) -> usize {
        self.memory_size
    }

    /// The remembered info of `term`, `Some(None)` for a term not in the dictionary.
    ///
    /// A term is remembered after its dictionary block was read through the shared pin, which
    /// keeps the block: the synchronous lookups of a search find it pinned.
    pub fn term_info(&self, term: &Term) -> Option<Option<TermInfo>> {
        self.term_infos.read().get(term)
    }

    /// Remembers the info of `term`, unless it is too long or the searcher holds as many terms
    /// as it remembers (`RememberedTerms::insert`).
    pub fn remember_term_info(&self, term: &Term, info: Option<TermInfo>) {
        let Some(heap_bytes) = RememberedTerms::heap_bytes(term) else {
            return;
        };
        let term = term.clone();
        self.term_infos.write().insert(term, heap_bytes, info);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn term(text: &str) -> Term {
        Term::from_field_text(Field::from_field_id(0), text)
    }

    fn remember(terms: &mut RememberedTerms, text: &str) {
        let term = term(text);
        let heap_bytes = RememberedTerms::heap_bytes(&term).unwrap();
        terms.insert(term, heap_bytes, None);
    }

    /// Full, the table is no larger than charged. It keeps its terms, mostly the frequent ones
    /// seen first, until it has refused a few times as many: then it starts over.
    #[test]
    fn test_remembered_terms_stay_within_their_table() {
        let mut terms = RememberedTerms::default();
        for i in 0..MAX_REMEMBERED_TERMS {
            remember(&mut terms, &i.to_string());
        }
        assert_eq!(terms.infos.len(), MAX_REMEMBERED_TERMS);
        assert!(terms.infos.capacity() <= MAX_REMEMBERED_TERMS);

        remember(&mut terms, "refused");
        assert_eq!(terms.get(&term("refused")), None);
        // A remembered term is updated, and does not count as refused.
        let info = TermInfo {
            doc_freq: 7,
            postings_range: 0..1,
            positions_range: 0..0,
        };
        terms.insert(term("0"), 16, Some(info.clone()));
        assert_eq!(terms.get(&term("0")), Some(Some(info)));
        for i in 2..REFUSED_TERMS_BEFORE_RESET {
            remember(&mut terms, &format!("refused{i}"));
        }
        assert_eq!(terms.infos.len(), MAX_REMEMBERED_TERMS);

        remember(&mut terms, "after");
        assert_eq!(terms.infos.len(), 1);
        assert_eq!(terms.get(&term("after")), Some(None));
        assert_eq!(terms.get(&term("0")), None);
        assert!(terms.infos.capacity() <= MAX_REMEMBERED_TERMS);
    }

    /// Long terms are not remembered, and the terms remembered take a bounded heap.
    #[test]
    fn test_remembered_terms_bound_their_heap_bytes() {
        let longest = "x".repeat(MAX_REMEMBERED_TERM_BYTES - 1);
        assert_eq!(RememberedTerms::heap_bytes(&term("a")), Some(16));
        assert_eq!(RememberedTerms::heap_bytes(&term(&longest)), Some(128));
        assert_eq!(RememberedTerms::heap_bytes(&term(&(longest + "x"))), None);

        let mut terms = RememberedTerms::default();
        let fit = MAX_REMEMBERED_TERM_HEAP_BYTES / MAX_REMEMBERED_TERM_BYTES;
        for i in 0..=fit {
            remember(&mut terms, &format!("{i:0>127}"));
        }
        assert_eq!(terms.infos.len(), fit);
        assert_eq!(terms.heap_bytes, MAX_REMEMBERED_TERM_HEAP_BYTES);
        assert_eq!(terms.refused, 1);
    }
}
