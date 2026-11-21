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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::RwLock;
use tantivy::Index;
use tantivy::IndexReader;
use tantivy::ReloadPolicy;
use tantivy::Searcher;
use tantivy::SegmentReader;
use tantivy::Term;
use tantivy::directory::CompositeFile;
use tantivy::directory::FileSlice;
use tantivy::index::Segment;
use tantivy::index::SegmentComponent;
use tantivy::postings::TermInfo;
use tantivy::schema::Field;

use super::directory::FooterDirectory;

/// Numbers the searchers the process opens, each once.
static NEXT_SEARCHER_ID: AtomicU64 = AtomicU64::new(0);

/// The postings and positions of a field in the segment of a bundle.
#[derive(Clone)]
pub struct FieldPostings {
    pub postings: FileSlice,
    /// None when the segment has no positions of the field.
    pub positions: Option<FileSlice>,
}

/// The info of a term in the dictionary of an opened searcher, `None` for a term not in it,
/// cached under `InvertedIndexSearcher::term_key`.
#[derive(Clone, Debug)]
pub struct InvertedIndexTermInfo {
    pub info: Option<TermInfo>,
    /// Bytes of the key it is cached under. For a value this small, the key and the cache entry
    /// take most of what caching it takes: they are charged with it.
    pub key_len: usize,
}

/// An inverted index bundle opened once, shared by the searches of its block.
///
/// It reads through a shared `SearchPinDirectory` that keeps, for all searches, the ranges of
/// the files `retains` accepts: the term dictionary, fieldnorms and fast fields. A search runs
/// in `with_search_scope`, which pins the postings and positions it warms until it completes.
pub struct InvertedIndexSearcher {
    /// Unique to this opening of the bundle: see `term_key`.
    id: u64,
    searcher: Searcher,
    segment: Segment,
    directory: FooterDirectory,
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
            .saturating_add(size_of::<Self>());
        Ok(Self {
            id: NEXT_SEARCHER_ID.fetch_add(1, Ordering::Relaxed),
            searcher,
            segment,
            directory,
            field_postings: RwLock::new(HashMap::new()),
            memory_size,
        })
    }

    pub fn searcher(&self) -> &Searcher {
        &self.searcher
    }

    /// The reader of the single segment of the bundle.
    pub fn segment_reader(&self) -> &SegmentReader {
        // `open` checked that there is one.
        &self.searcher.segment_readers()[0]
    }

    pub fn segment(&self) -> &Segment {
        &self.segment
    }

    /// The directory over the shared pin, which also reads whole physical components.
    pub fn directory(&self) -> &FooterDirectory {
        &self.directory
    }

    /// What its memory is charged: the most it holds, once its pin keeps every retained file.
    pub fn memory_size(&self) -> usize {
        self.memory_size
    }

    /// The key of the cached info of `term` in the dictionary of this searcher.
    ///
    /// A search finding the info of a term cached skips looking the term up, the lookup that
    /// pins the dictionary block Tantivy's synchronous search reads. Only the shared pin of the
    /// searcher that looked the term up keeps that block: the key is unique to the searcher, and
    /// a searcher opened again from the same bundle looks its terms up again.
    pub fn term_key(&self, term: &Term) -> String {
        term_key(self.id, term)
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

/// The searcher and the field of `term` in decimal, then the bytes of its value in hex: they
/// need not be UTF-8.
fn term_key(searcher: u64, term: &Term) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let value = term.serialized_value_bytes();
    let mut key = format!("{searcher}:{}:", term.field().field_id());
    key.reserve(2 * value.len());
    for byte in value {
        key.push(char::from(HEX[usize::from(byte >> 4)]));
        key.push(char::from(HEX[usize::from(byte & 0xf)]));
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keys tell searchers, fields and value bytes apart; the bytes need not be UTF-8.
    #[test]
    fn test_term_keys_tell_searchers_fields_and_bytes_apart() {
        let body = Field::from_field_id(0);
        let title = Field::from_field_id(1);
        let text = Term::from_field_text(body, "ab");
        assert_eq!(term_key(7, &text), "7:0:6162");
        assert_ne!(term_key(8, &text), term_key(7, &text));
        let other_field = Term::from_field_text(title, "ab");
        assert_ne!(term_key(7, &other_field), term_key(7, &text));
        let bytes = Term::from_field_bytes(body, &[0xff, 0x00]);
        assert_eq!(term_key(7, &bytes), "7:0:ff00");
    }
}
