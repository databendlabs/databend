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

// Copyright (c) 2018 by the tantivy project authors
// (https://github.com/quickwit-oss/tantivy), as listed in the AUTHORS file.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::marker::PhantomData;
use std::ops::BitAndAssign;
use std::ops::BitOrAssign;
use std::ops::SubAssign;
use std::path::Path;
use std::path::PathBuf;
use std::result;
use std::sync::Arc;

use bytes::Bytes;
use crc32fast::Hasher;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::types::F32;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::testify_version;
use levenshtein_automata::DFA;
use levenshtein_automata::Distance;
use levenshtein_automata::LevenshteinAutomatonBuilder;
use log::warn;
use parquet::format::FileMetaData;
use roaring::RoaringTreemap;
use tantivy::Directory;
use tantivy::Term;
use tantivy::Version;
use tantivy::directory::AntiCallToken;
use tantivy::directory::FileHandle;
use tantivy::directory::FileSlice;
use tantivy::directory::OwnedBytes;
use tantivy::directory::TerminatingWrite;
use tantivy::directory::WatchCallback;
use tantivy::directory::WatchHandle;
use tantivy::directory::WritePtr;
use tantivy::directory::error::DeleteError;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::error::OpenWriteError;
use tantivy::fieldnorm::FieldNormReader;
use tantivy::positions::PositionReader;
use tantivy::postings::BlockSegmentPostings;
use tantivy::postings::TermInfo;
use tantivy::query::AllQuery;
use tantivy::query::Bm25StatisticsProvider;
use tantivy::query::Bm25Weight;
use tantivy::query::BooleanQuery;
use tantivy::query::BoostQuery;
use tantivy::query::ConstScoreQuery;
use tantivy::query::EmptyQuery;
use tantivy::query::FuzzyTermQuery;
use tantivy::query::Occur;
use tantivy::query::PhrasePrefixQuery;
use tantivy::query::PhraseQuery;
use tantivy::query::Query;
use tantivy::query::QueryClone;
use tantivy::query::TermQuery;
use tantivy::schema::Field;
use tantivy::version;
use tantivy_common::BinarySerializable;
use tantivy_common::HasLen;
use tantivy_common::VInt;
use tantivy_fst::Automaton;
use tantivy_fst::IntoStreamer;
use tantivy_fst::Regex;
use tantivy_fst::Streamer;
// tantivy version is used to generate the footer data

// The magic byte of the footer to identify corruption
// or an old version of the footer.
const FOOTER_MAGIC_NUMBER: u32 = 1337;

type CrcHashU32 = u32;

/// A Footer is appended every part of data, like tantivy file.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Footer {
    version: Version,
    crc: CrcHashU32,
}

impl Footer {
    fn new(crc: CrcHashU32) -> Self {
        let version = version().clone();
        Footer { version, crc }
    }

    fn append_footer<W: std::io::Write>(&self, write: &mut W) -> Result<()> {
        let footer_payload_len = write.write(serde_json::to_string(&self)?.as_ref())?;
        BinarySerializable::serialize(&(footer_payload_len as u32), write)?;
        BinarySerializable::serialize(&FOOTER_MAGIC_NUMBER, write)?;
        Ok(())
    }
}

// Build footer for tantivy files.
// Footer is used to check whether the data is valid when open a file.
pub fn build_tantivy_footer(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    let crc = hasher.finalize();
    let footer = Footer::new(crc);
    let mut buf = Vec::new();
    footer.append_footer(&mut buf)?;
    Ok(buf)
}

fn extract_footer(data: FileSlice) -> Result<(Vec<FileAddr>, Vec<usize>)> {
    // The following code is copied from tantivy `CompositeFile::open` function.
    // extract field number and offsets of each fields.
    let end = data.len();
    let footer_len_data = data.slice_from(end - 4).read_bytes()?;
    let footer_len = u32::deserialize(&mut footer_len_data.as_slice())? as usize;
    let footer_start = end - 4 - footer_len;
    let footer_data = data
        .slice(footer_start..footer_start + footer_len)
        .read_bytes()?;
    let mut footer_buffer = footer_data.as_slice();
    let num_fields = VInt::deserialize(&mut footer_buffer)?.0 as usize;

    let mut offset = 0;
    let mut offsets = Vec::with_capacity(num_fields);
    let mut file_addrs = Vec::with_capacity(num_fields);
    for _ in 0..num_fields {
        offset += VInt::deserialize(&mut footer_buffer)?.0 as usize;
        offsets.push(offset);
        let file_addr = FileAddr::deserialize(&mut footer_buffer)?;
        file_addrs.push(file_addr);
    }
    offsets.push(footer_start);

    Ok((file_addrs, offsets))
}

// Extract fsts and term dicts into separate columns.
pub fn extract_fsts(
    data: FileSlice,
    fields: &mut Vec<TableField>,
    values: &mut Vec<Scalar>,
) -> Result<()> {
    let (file_addrs, offsets) = extract_footer(data.clone())?;

    for (i, file_addr) in file_addrs.iter().enumerate() {
        let field_id = file_addr.field.field_id();
        let start_offset = offsets[i];
        let end_offset = offsets[i + 1];

        let field_slice = data.slice(start_offset..end_offset);

        let (main_slice, footer_len_slice) = field_slice.split_from_end(16);
        let mut footer_len_bytes = footer_len_slice.read_bytes()?;
        let footer_size = u64::deserialize(&mut footer_len_bytes)?;

        let (fst_file_slice, term_dict_file_slice) =
            main_slice.split_from_end(footer_size as usize);

        let fst_field_name = format!("fst-{}", field_id);
        let fst_field = TableField::new(&fst_field_name, TableDataType::Binary);
        fields.push(fst_field);

        let fst_bytes = fst_file_slice.read_bytes()?;
        values.push(Scalar::Binary(fst_bytes.as_slice().to_vec()));

        let term_dict_field_name = format!("term-{}", field_id);
        let term_dict_field = TableField::new(&term_dict_field_name, TableDataType::Binary);
        fields.push(term_dict_field);

        let term_dict_bytes = term_dict_file_slice.read_bytes()?;
        values.push(Scalar::Binary(term_dict_bytes.as_slice().to_vec()));
    }

    Ok(())
}

// Extract component file into separate columns by fields.
pub fn extract_component_fields(
    name: &str,
    data: FileSlice,
    fields: &mut Vec<TableField>,
    values: &mut Vec<Scalar>,
) -> Result<()> {
    let (file_addrs, offsets) = extract_footer(data.clone())?;

    for (i, file_addr) in file_addrs.iter().enumerate() {
        let field_id = file_addr.field.field_id();
        let start_offset = offsets[i];
        let end_offset = offsets[i + 1];

        let field_name = format!("{}-{}", name, field_id);
        let field = TableField::new(&field_name, TableDataType::Binary);
        fields.push(field);

        let field_slice = data.slice(start_offset..end_offset);
        let field_bytes = field_slice.read_bytes()?;
        values.push(Scalar::Binary(field_bytes.as_slice().to_vec()));
    }

    Ok(())
}

#[derive(Eq, PartialEq, Hash, Copy, Ord, PartialOrd, Clone, Debug)]
struct FileAddr {
    field: Field,
    idx: usize,
}

impl BinarySerializable for FileAddr {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.field.serialize(writer)?;
        VInt(self.idx as u64).serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let field = Field::deserialize(reader)?;
        let idx = VInt::deserialize(reader)?.0 as usize;
        Ok(FileAddr { field, idx })
    }
}

struct DfaWrapper(pub DFA);

impl Automaton for DfaWrapper {
    type State = u32;

    fn start(&self) -> Self::State {
        self.0.initial_state()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        match self.0.distance(*state) {
            Distance::Exact(_) => true,
            Distance::AtLeast(_) => false,
        }
    }

    fn can_match(&self, state: &u32) -> bool {
        *state != levenshtein_automata::SINK_STATE
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0.transition(*state, byte)
    }
}

// Read term related infos.
#[derive(Clone)]
pub struct TermReader {
    row_count: u64,
    need_position: bool,
    has_score: bool,
    // key is `term`, value is `term_id`,
    // These terms are in `fst`, means that those terms exist in the block,
    // we need to use related term infos to determine the matched `doc_ids`.
    // Use `term_id` as key to get related information and avoid copy `term`.
    term_map: HashMap<Term, u64>,
    // key is `term_id`, value is `field_id`.
    term_field_id_map: HashMap<u64, u32>,
    // key is `term_id`, value is `term_info`.
    term_infos: HashMap<u64, TermInfo>,
    // key is `term_id`, value is related `BlockSegmentPostings`,
    // used to read `doc_ids` and `term_freqs`.
    block_postings_map: HashMap<u64, BlockSegmentPostings>,
    // key is `term_id`, value is related `PositionReader`,
    // used to read `positions` in each docs.
    position_reader_map: HashMap<u64, PositionReader>,
    // key is `term_id`, value is related `doc_ids`.
    // `doc_ids` is lazy loaded when used.
    doc_ids: HashMap<u64, RoaringTreemap>,
    // key is `term_id`, value is related `term_freqs`.
    // `term_freqs` is lazy loaded when used.
    term_freqs: HashMap<u64, Vec<u32>>,
    // key is `field_id`, value is the `FieldNormReader`, used to read fieldnorm.
    fieldnorm_reader_map: HashMap<u32, FieldNormReader>,
    // key is `field_id`, value is the number of tokens.
    field_num_tokens_map: HashMap<u32, u64>,
}

impl TermReader {
    pub fn create(
        row_count: u64,
        need_position: bool,
        has_score: bool,
        terms: HashMap<Term, (u32, u64)>,
        term_infos: HashMap<u64, TermInfo>,
        block_postings_map: HashMap<u64, BlockSegmentPostings>,
        position_reader_map: HashMap<u64, PositionReader>,
        fieldnorm_reader_map: HashMap<u32, FieldNormReader>,
        field_num_tokens_map: HashMap<u32, u64>,
    ) -> Self {
        let term_len = terms.len();
        let term_field_id_map = terms
            .iter()
            .map(|(_, (field_id, term_id))| (*term_id, *field_id))
            .collect::<HashMap<u64, u32>>();
        let term_map = terms
            .into_iter()
            .map(|(term, (_, term_id))| (term, term_id))
            .collect::<HashMap<Term, u64>>();

        Self {
            row_count,
            need_position,
            has_score,
            term_map,
            term_field_id_map,
            term_infos,
            block_postings_map,
            position_reader_map,
            doc_ids: HashMap::with_capacity(term_len),
            term_freqs: HashMap::with_capacity(term_len),
            fieldnorm_reader_map,
            field_num_tokens_map,
        }
    }

    // get `doc_ids` of a `term_id`,
    fn get_doc_ids(&mut self, term_id: u64) -> Result<&RoaringTreemap> {
        if let std::collections::hash_map::Entry::Vacant(doc_ids_entry) =
            self.doc_ids.entry(term_id)
        {
            // `doc_ids` are lazy loaded when used.
            let block_postings = self.block_postings_map.get_mut(&term_id).ok_or_else(|| {
                ErrorCode::TantivyError(format!(
                    "inverted index block postings `{}` does not exist",
                    term_id
                ))
            })?;

            let term_freqs_len = if self.need_position || self.has_score {
                let term_info = self.term_infos.get(&term_id).ok_or_else(|| {
                    ErrorCode::TantivyError(format!(
                        "inverted index term info `{}` does not exist",
                        term_id
                    ))
                })?;
                term_info.doc_freq as usize
            } else {
                0
            };
            let mut doc_ids = RoaringTreemap::new();
            let mut term_freqs = Vec::with_capacity(term_freqs_len);
            // `doc_ids` are stored in multiple blocks and need to be decode sequentially.
            // TODO: We can skip some blocks by checking related `doc_ids`.
            loop {
                let block_doc_ids = block_postings.docs();
                if block_doc_ids.is_empty() {
                    break;
                }
                doc_ids
                    .append(block_doc_ids.iter().map(|id| *id as u64))
                    .unwrap();

                // `term_freqs` is only used if the query need position or score.
                if self.need_position || self.has_score {
                    let block_term_freqs = block_postings.freqs();
                    term_freqs.extend_from_slice(block_term_freqs);
                }
                block_postings.advance();
            }
            doc_ids_entry.insert(doc_ids);
            self.term_freqs.insert(term_id, term_freqs);
        }

        let doc_ids = self.doc_ids.get(&term_id).unwrap();
        Ok(doc_ids)
    }

    // get the position `offsets` and `term_freqs` of a `term_id` in each `docs`,
    // which is used to read `positions`.
    fn get_position_offsets(
        &mut self,
        term_id: u64,
        all_doc_ids: &RoaringTreemap,
    ) -> Result<HashMap<u64, (u64, u32)>> {
        let doc_ids = self.doc_ids.get(&term_id).unwrap();
        let term_freqs = self.term_freqs.get(&term_id).unwrap();

        let mut doc_offset = 0;
        let mut offset_and_term_freqs = HashMap::with_capacity(all_doc_ids.len() as usize);
        for (doc_id, term_freq) in doc_ids.iter().zip(term_freqs.iter()) {
            if all_doc_ids.len() as usize == offset_and_term_freqs.len() {
                break;
            }
            if !all_doc_ids.contains(doc_id) {
                doc_offset += *term_freq as u64;
                continue;
            }

            offset_and_term_freqs.insert(doc_id, (doc_offset, *term_freq));
            doc_offset += *term_freq as u64;
        }

        Ok(offset_and_term_freqs)
    }

    // get `positions` of a `term_id` in a `doc`.
    fn get_positions(
        &mut self,
        term_id: u64,
        doc_offset: u64,
        term_freq: u32,
    ) -> Result<RoaringTreemap> {
        let position_reader = self.position_reader_map.get_mut(&term_id).ok_or_else(|| {
            ErrorCode::TantivyError(format!(
                "inverted index position reader `{}` does not exist",
                term_id
            ))
        })?;

        let mut positions = vec![0; term_freq as usize];
        position_reader.read(doc_offset, &mut positions[..]);
        for i in 1..positions.len() {
            positions[i] += positions[i - 1];
        }
        let term_poses =
            RoaringTreemap::from_sorted_iter(positions.into_iter().map(|i| i as u64)).unwrap();
        Ok(term_poses)
    }

    fn term_id(&self, term: &Term) -> Option<&u64> {
        self.term_map.get(term)
    }

    fn field_id(&self, term_id: &u64) -> Option<&u32> {
        self.term_field_id_map.get(term_id)
    }

    fn fieldnorm_id(&self, field_id: u32, doc_id: u64) -> u8 {
        if let Some(fieldnorm_reader) = self.fieldnorm_reader_map.get(&field_id) {
            fieldnorm_reader.fieldnorm_id(doc_id as u32)
        } else {
            1
        }
    }

    fn term_freq(&self, term_id: u64, doc_id: u64) -> Option<&u32> {
        if let (Some(doc_ids), Some(term_freqs)) =
            (self.doc_ids.get(&term_id), self.term_freqs.get(&term_id))
        {
            if doc_ids.contains(doc_id) {
                // if not store `term_freq`, return 1 as default value.
                if term_freqs.is_empty() {
                    return Some(&1);
                }
                let rank = doc_ids.rank(doc_id) as usize;
                if rank > 0 {
                    let idx = rank - 1;
                    return term_freqs.get(idx);
                }
            }
        }
        None
    }
}

// impl `Bm25StatisticsProvider` for `TermReader` to compute BM25 scores.
// Note: The numbers are within a block, not global numbers,
// so the result score is not exact.
impl Bm25StatisticsProvider for TermReader {
    // The total number of tokens in a given field across all documents in the block.
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        let field_id = field.field_id();
        if let Some(total_num_tokens) = self.field_num_tokens_map.get(&field_id) {
            Ok(*total_num_tokens)
        } else {
            Ok(1)
        }
    }

    // The total number of documents in the block.
    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.row_count)
    }

    // The number of documents containing the given term in the block.
    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        if let Some(term_id) = self.term_map.get(term) {
            if let Some(term_info) = self.term_infos.get(term_id) {
                return Ok(term_info.doc_freq as u64);
            }
        }
        Ok(1)
    }
}

// Collect matched `doc_ids` for a Query.
#[derive(Clone)]
pub struct DocIdsCollector {
    term_reader: TermReader,
    // key is phrase query, value is `doc_id` and phrase count
    query_phrase_counts_map: HashMap<String, HashMap<u64, u64>>,
    // key is fuzzy query, value is `doc_ids`
    query_fuzzy_map: HashMap<String, RoaringTreemap>,
}

impl DocIdsCollector {
    pub fn create(term_reader: TermReader) -> Self {
        Self {
            term_reader,
            query_phrase_counts_map: HashMap::new(),
            query_fuzzy_map: HashMap::new(),
        }
    }

    fn check_term_match(
        fst_map: &tantivy_fst::Map<OwnedBytes>,
        field_id: u32,
        term: &Term,
        matched_terms: &mut HashMap<Term, (u32, u64)>,
    ) -> bool {
        if let Some(term_id) = fst_map.get(term.serialized_value_bytes()) {
            matched_terms.insert(term.clone(), (field_id, term_id));
            true
        } else {
            false
        }
    }

    fn get_fst_map(
        field_id: u32,
        fst_maps: &HashMap<u32, tantivy_fst::Map<OwnedBytes>>,
    ) -> Result<&tantivy_fst::Map<OwnedBytes>> {
        let fst_map = fst_maps.get(&field_id).ok_or_else(|| {
            ErrorCode::TantivyError(format!(
                "inverted index fst field `{}` does not exist",
                field_id
            ))
        })?;

        Ok(fst_map)
    }

    // Check if fst contains terms in query.
    // If not, we can skip read other parts of inverted index.
    pub fn check_term_fsts_match(
        query: Box<dyn Query>,
        fst_maps: &HashMap<u32, tantivy_fst::Map<OwnedBytes>>,
        fuzziness: &Option<u8>,
        matched_terms: &mut HashMap<Term, (u32, u64)>,
        prefix_terms: &mut HashMap<Term, Vec<u64>>,
        fuzziness_terms: &mut HashMap<Term, Vec<u64>>,
    ) -> Result<bool> {
        if let Some(term_query) = query.downcast_ref::<TermQuery>() {
            let term = term_query.term();
            let field = term.field();
            let field_id = field.field_id();
            let fst_map = Self::get_fst_map(field_id, fst_maps)?;
            let matched = Self::check_term_match(fst_map, field_id, term, matched_terms);
            Ok(matched)
        } else if let Some(bool_query) = query.downcast_ref::<BooleanQuery>() {
            let mut matched_any = false;
            for (occur, sub_query) in bool_query.clauses() {
                let matched = Self::check_term_fsts_match(
                    sub_query.box_clone(),
                    fst_maps,
                    fuzziness,
                    matched_terms,
                    prefix_terms,
                    fuzziness_terms,
                )?;
                match occur {
                    Occur::Should => {
                        if matched {
                            matched_any = true;
                        }
                    }
                    Occur::Must => {
                        if matched {
                            matched_any = true;
                        } else {
                            return Ok(false);
                        }
                    }
                    Occur::MustNot => {
                        // Matched means that the block contains the term,
                        // but we still need to filter out the `doc_ids`.
                        matched_any = true;
                    }
                }
            }
            Ok(matched_any)
        } else if let Some(phrase_query) = query.downcast_ref::<PhraseQuery>() {
            // PhraseQuery must match all terms.
            let field = phrase_query.field();
            let field_id = field.field_id();
            let fst_map = Self::get_fst_map(field_id, fst_maps)?;

            let mut matched_all = true;
            for term in phrase_query.phrase_terms() {
                let matched = Self::check_term_match(fst_map, field_id, &term, matched_terms);
                if !matched {
                    matched_all = false;
                    break;
                }
            }
            Ok(matched_all)
        } else if let Some(phrase_prefix_query) = query.downcast_ref::<PhrasePrefixQuery>() {
            // PhrasePrefixQuery must match all terms.
            let field = phrase_prefix_query.field();
            let field_id = field.field_id();
            let fst_map = Self::get_fst_map(field_id, fst_maps)?;

            let mut matched_all = true;
            for term in phrase_prefix_query.phrase_terms() {
                let matched = Self::check_term_match(fst_map, field_id, &term, matched_terms);
                if !matched {
                    matched_all = false;
                    break;
                }
            }
            if !matched_all {
                return Ok(false);
            }

            // using regex to check prefix term, get related term ids.
            let (_, prefix_term) = phrase_prefix_query.prefix_term_with_offset();
            let term_str = String::from_utf8_lossy(prefix_term.serialized_value_bytes());
            let key = format!("{}.*", term_str);
            let re = Regex::new(&key).map_err(|_| {
                ErrorCode::TantivyError(format!("inverted index create regex `{}` failed", key))
            })?;

            let mut prefix_term_ids = vec![];
            let mut stream = fst_map.search(&re).into_stream();
            while let Some((key, term_id)) = stream.next() {
                let key_str = unsafe { std::str::from_utf8_unchecked(key) };
                let prefix_term = Term::from_field_text(field, key_str);
                matched_terms.insert(prefix_term.clone(), (field_id, term_id));
                prefix_term_ids.push(term_id);
            }
            let matched = !prefix_term_ids.is_empty();
            if matched {
                prefix_terms.insert(prefix_term.clone(), prefix_term_ids);
            }
            Ok(matched)
        } else if let Some(fuzzy_term_query) = query.downcast_ref::<FuzzyTermQuery>() {
            // FuzzyTermQuery match terms by levenshtein distance.
            let fuzziness = fuzziness.unwrap();
            let term = fuzzy_term_query.term();
            let field = term.field();
            let field_id = field.field_id();
            let fst_map = Self::get_fst_map(field_id, fst_maps)?;

            // build levenshtein automaton to check fuzziness term, get related term ids.
            let lev_automaton_builder = LevenshteinAutomatonBuilder::new(fuzziness, true);
            let term_str = String::from_utf8_lossy(term.serialized_value_bytes());
            let automaton = DfaWrapper(lev_automaton_builder.build_dfa(&term_str));

            let mut fuzz_term_ids = vec![];
            let mut stream = fst_map.search(automaton).into_stream();
            while let Some((key, term_id)) = stream.next() {
                let key_str = unsafe { std::str::from_utf8_unchecked(key) };
                let fuzz_term = Term::from_field_text(field, key_str);
                matched_terms.insert(fuzz_term.clone(), (field_id, term_id));
                fuzz_term_ids.push(term_id);
            }
            let matched = !fuzz_term_ids.is_empty();
            if matched {
                fuzziness_terms.insert(term.clone(), fuzz_term_ids);
            }
            Ok(matched)
        } else if let Some(boost_query) = query.downcast_ref::<BoostQuery>() {
            Self::check_term_fsts_match(
                boost_query.underlying_query(),
                fst_maps,
                fuzziness,
                matched_terms,
                prefix_terms,
                fuzziness_terms,
            )
        } else if let Some(const_query) = query.downcast_ref::<ConstScoreQuery>() {
            Self::check_term_fsts_match(
                const_query.underlying_query(),
                fst_maps,
                fuzziness,
                matched_terms,
                prefix_terms,
                fuzziness_terms,
            )
        } else if let Some(_empty_query) = query.downcast_ref::<EmptyQuery>() {
            Ok(false)
        } else if let Some(_all_query) = query.downcast_ref::<AllQuery>() {
            Ok(true)
        } else {
            Err(ErrorCode::TantivyError(format!(
                "inverted index unsupported query `{:?}`",
                query
            )))
        }
    }

    // The phrase query matches `doc_ids` as follows:
    //
    // 1. Collect the position for each term in the query.
    // 2. Collect the `doc_ids` of each term and take
    //    the intersection to get the candidate `doc_ids`.
    // 3. Iterate over the candidate `doc_ids` to check whether
    //    the position of terms matches the position of terms in query.
    // 4. Each position in the first term is a possible query phrase beginning.
    //    Verify that the beginning is valid by checking whether corresponding
    //    positions in other terms exist. If not, delete the possible position
    //    in the first term. After traversing all terms, determine if there are
    //    any positions left in the first term. If there are, then the `doc_id`
    //    is matched.
    //
    // If the query is a prefix phrase query, also check if any prefix terms
    // match the positions.
    pub fn collect_phrase_matched_doc_ids(
        &mut self,
        query_key: String,
        phrase_terms: &[(usize, Term)],
        prefix_term: Option<(usize, &Vec<u64>)>,
    ) -> Result<Option<RoaringTreemap>> {
        let mut query_term_poses = Vec::with_capacity(phrase_terms.len());
        for (term_pos, term) in phrase_terms {
            // term not exist means this phrase in not matched.
            let Some(term_id) = self.term_reader.term_id(term) else {
                return Ok(None);
            };
            query_term_poses.push((*term_pos, *term_id));
        }
        if query_term_poses.is_empty() {
            return Ok(None);
        }

        let first_term_pos = &query_term_poses[0].0;
        let first_term_id = &query_term_poses[0].1;

        let mut term_ids = HashSet::with_capacity(phrase_terms.len() + 1);
        term_ids.insert(*first_term_id);

        let first_doc_ids = self.term_reader.get_doc_ids(*first_term_id)?;
        let mut candidate_doc_ids = RoaringTreemap::new();
        candidate_doc_ids.bitor_assign(first_doc_ids);

        // Collect the `doc_ids` of other terms in the query, and take the intersection,
        // obtains the candidate `doc_ids` containing all terms.
        let mut query_term_offsets = Vec::with_capacity(query_term_poses.len() - 1);
        for (term_pos, term_id) in query_term_poses.iter().skip(1) {
            if !term_ids.contains(term_id) {
                let doc_ids = self.term_reader.get_doc_ids(*term_id)?;

                candidate_doc_ids.bitand_assign(doc_ids);
                if candidate_doc_ids.is_empty() {
                    break;
                }
                term_ids.insert(*term_id);
            }
            let term_pos_offset = (term_pos - first_term_pos) as u64;
            query_term_offsets.push((*term_id, term_pos_offset));
        }
        // If the candidate `doc_ids` is empty, all docs are not matched.
        if candidate_doc_ids.is_empty() {
            return Ok(None);
        }

        // If the query is a prefix phrase query,
        // also need to collect `doc_ids` of prefix terms.
        if let Some((_, prefix_term_ids)) = prefix_term {
            let mut all_prefix_doc_ids = RoaringTreemap::new();
            for prefix_term_id in prefix_term_ids {
                let prefix_doc_ids = self.term_reader.get_doc_ids(*prefix_term_id)?;
                // If the `doc_ids` does not intersect at all, this prefix can be ignored.
                if candidate_doc_ids.is_disjoint(prefix_doc_ids) {
                    continue;
                }

                all_prefix_doc_ids.bitor_assign(prefix_doc_ids);
                term_ids.insert(*prefix_term_id);
            }
            // If there is no matched prefix `doc_ids`, the prefix term does not matched.
            if all_prefix_doc_ids.is_empty() {
                return Ok(None);
            }

            // Get the intersection of phrase `doc_ids` and prefix `doc_ids`
            candidate_doc_ids.bitand_assign(all_prefix_doc_ids);
        }

        // Collect the position `offset` and `term_freqs` for each terms,
        // which can be used to read positions.
        let mut offset_and_term_freqs_map = HashMap::new();
        for term_id in term_ids.into_iter() {
            let offset_and_term_freqs = self
                .term_reader
                .get_position_offsets(term_id, &candidate_doc_ids)?;
            offset_and_term_freqs_map.insert(term_id, offset_and_term_freqs);
        }

        let first_offset_and_term_freqs = offset_and_term_freqs_map.get(first_term_id).unwrap();

        // Check every candidate `doc_ids` if the position of each terms match the query.
        let mut all_doc_ids = RoaringTreemap::new();
        let mut offset_poses = RoaringTreemap::new();
        let mut phrase_counts_map = HashMap::new();
        let mut term_poses_map = HashMap::new();
        for (doc_id, (first_doc_offset, first_term_freq)) in first_offset_and_term_freqs.iter() {
            let mut first_term_poses = self.term_reader.get_positions(
                *first_term_id,
                *first_doc_offset,
                *first_term_freq,
            )?;

            term_poses_map.clear();
            term_poses_map.insert(first_term_id, first_term_poses.clone());

            for (term_id, term_pos_offset) in &query_term_offsets {
                if !term_poses_map.contains_key(term_id) {
                    let offset_and_term_freqs = offset_and_term_freqs_map.get(term_id).unwrap();
                    let (doc_offset, term_freq) = offset_and_term_freqs.get(doc_id).unwrap();

                    let term_poses =
                        self.term_reader
                            .get_positions(*term_id, *doc_offset, *term_freq)?;
                    term_poses_map.insert(term_id, term_poses);
                }
                let term_poses = term_poses_map.get(term_id).unwrap();

                // Using the position of the first term and the offset of this term with the first term,
                // calculate all possible positions for this term.
                offset_poses.clear();
                offset_poses
                    .append(first_term_poses.iter().map(|pos| pos + term_pos_offset))
                    .unwrap();

                // Term possible positions subtract term actual positions,
                // remaining positions are not matched and need to be removed in the first term.
                offset_poses.sub_assign(term_poses);
                for offset_pos in &offset_poses {
                    first_term_poses.remove(offset_pos - term_pos_offset);
                }
                if first_term_poses.is_empty() {
                    break;
                }
            }

            // If the query is a prefix phrase query,
            // also need to check if any prefix term match.
            if let Some((prefix_term_pos, prefix_term_ids)) = prefix_term {
                if !first_term_poses.is_empty() {
                    let mut prefix_matched = false;
                    let prefix_term_pos_offset = (prefix_term_pos - first_term_pos) as u64;
                    for prefix_term_id in prefix_term_ids {
                        if !term_poses_map.contains_key(prefix_term_id) {
                            if let Some(offset_and_term_freqs) =
                                offset_and_term_freqs_map.get(prefix_term_id)
                            {
                                if let Some((doc_offset, term_freq)) =
                                    offset_and_term_freqs.get(doc_id)
                                {
                                    let term_poses = self.term_reader.get_positions(
                                        *prefix_term_id,
                                        *doc_offset,
                                        *term_freq,
                                    )?;
                                    term_poses_map.insert(prefix_term_id, term_poses);
                                }
                            }
                        }
                        if let Some(term_poses) = term_poses_map.get(prefix_term_id) {
                            offset_poses.clear();
                            offset_poses
                                .append(
                                    first_term_poses
                                        .iter()
                                        .map(|pos| pos + prefix_term_pos_offset),
                                )
                                .unwrap();

                            offset_poses.bitand_assign(term_poses);
                            // If any of the possible prefix term positions exist,
                            // the prefix phrase query is matched.
                            if !offset_poses.is_empty() {
                                prefix_matched = true;
                                break;
                            }
                        }
                    }
                    if prefix_matched {
                        all_doc_ids.insert(*doc_id);
                        if self.term_reader.has_score {
                            phrase_counts_map.insert(*doc_id, first_term_poses.len());
                        }
                    }
                }
            } else {
                let matched = !first_term_poses.is_empty();
                if matched {
                    all_doc_ids.insert(*doc_id);
                    if self.term_reader.has_score {
                        // Using the number of the first term as the phrase count,
                        // it's not very exact, but in order to simplify the calculation,
                        // we can ignore the difference.
                        phrase_counts_map.insert(*doc_id, first_term_poses.len());
                    }
                }
            }
        }
        if self.term_reader.has_score {
            self.query_phrase_counts_map
                .insert(query_key, phrase_counts_map);
        }
        if !all_doc_ids.is_empty() {
            Ok(Some(all_doc_ids))
        } else {
            Ok(None)
        }
    }

    // collect matched doc ids by the query.
    pub fn collect_matched_doc_ids(
        &mut self,
        query: Box<dyn Query>,
        prefix_terms: &HashMap<Term, Vec<u64>>,
        fuzziness_terms: &HashMap<Term, Vec<u64>>,
    ) -> Result<Option<RoaringTreemap>> {
        if let Some(term_query) = query.downcast_ref::<TermQuery>() {
            let term = term_query.term();
            if let Some(term_id) = self.term_reader.term_id(term) {
                let doc_ids = self.term_reader.get_doc_ids(*term_id)?;
                Ok(Some(doc_ids.clone()))
            } else {
                Ok(None)
            }
        } else if let Some(bool_query) = query.downcast_ref::<BooleanQuery>() {
            let mut should_doc_ids: Option<RoaringTreemap> = None;
            let mut must_doc_ids: Option<RoaringTreemap> = None;
            let mut must_not_doc_ids: Option<RoaringTreemap> = None;
            for (occur, sub_query) in bool_query.clauses() {
                let doc_ids = self.collect_matched_doc_ids(
                    sub_query.box_clone(),
                    prefix_terms,
                    fuzziness_terms,
                )?;
                if doc_ids.is_none() {
                    match occur {
                        Occur::Should => {
                            continue;
                        }
                        Occur::Must => {
                            if must_doc_ids.is_none() {
                                must_doc_ids = Some(RoaringTreemap::new());
                            }
                            continue;
                        }
                        Occur::MustNot => {
                            if must_not_doc_ids.is_none() {
                                must_not_doc_ids = Some(RoaringTreemap::new());
                            }
                            continue;
                        }
                    }
                }
                let doc_ids = doc_ids.unwrap();
                match occur {
                    Occur::Should => {
                        if let Some(ref mut should_doc_ids) = should_doc_ids {
                            should_doc_ids.bitor_assign(&doc_ids);
                        } else {
                            should_doc_ids = Some(doc_ids);
                        }
                    }
                    Occur::Must => {
                        if let Some(ref mut must_doc_ids) = must_doc_ids {
                            must_doc_ids.bitand_assign(&doc_ids);
                        } else {
                            must_doc_ids = Some(doc_ids);
                        }
                    }
                    Occur::MustNot => {
                        if let Some(ref mut must_not_doc_ids) = must_not_doc_ids {
                            must_not_doc_ids.bitor_assign(&doc_ids);
                        } else {
                            must_not_doc_ids = Some(doc_ids);
                        }
                    }
                }
            }

            let all_doc_ids = match (should_doc_ids, must_doc_ids, must_not_doc_ids) {
                // only should
                (Some(should_doc_ids), None, None) => should_doc_ids,
                // only must
                // should and must
                (_, Some(must_doc_ids), None) => must_doc_ids,
                // only must not
                (None, None, Some(must_not_doc_ids)) => {
                    let mut all_doc_ids = RoaringTreemap::from_iter(0..self.term_reader.row_count);
                    all_doc_ids.sub_assign(must_not_doc_ids);
                    all_doc_ids
                }
                // should and must not
                (Some(mut should_doc_ids), None, Some(must_not_doc_ids)) => {
                    should_doc_ids.sub_assign(must_not_doc_ids);
                    should_doc_ids
                }
                // must and must not
                // should, must and must not
                (_, Some(mut must_doc_ids), Some(must_not_doc_ids)) => {
                    must_doc_ids.sub_assign(must_not_doc_ids);
                    must_doc_ids
                }
                (None, None, None) => {
                    return Ok(None);
                }
            };

            if !all_doc_ids.is_empty() {
                Ok(Some(all_doc_ids))
            } else {
                Ok(None)
            }
        } else if let Some(phrase_query) = query.downcast_ref::<PhraseQuery>() {
            let query_key = format!("{:?}", phrase_query);
            let phrase_terms = phrase_query.get_phrase_terms_with_offsets();
            self.collect_phrase_matched_doc_ids(query_key, phrase_terms, None)
        } else if let Some(phrase_prefix_query) = query.downcast_ref::<PhrasePrefixQuery>() {
            let query_key = format!("{:?}", phrase_prefix_query);
            let phrase_terms = phrase_prefix_query.get_phrase_terms_with_offsets();
            let (prefix_term_pos, prefix_term) = phrase_prefix_query.prefix_term_with_offset();

            let Some(prefix_term_ids) = prefix_terms.get(&prefix_term) else {
                return Ok(None);
            };
            let prefix_term = Some((prefix_term_pos, prefix_term_ids));

            self.collect_phrase_matched_doc_ids(query_key, phrase_terms, prefix_term)
        } else if let Some(fuzzy_term_query) = query.downcast_ref::<FuzzyTermQuery>() {
            let mut all_doc_ids = RoaringTreemap::new();
            let term = fuzzy_term_query.term();

            let Some(fuzz_term_ids) = fuzziness_terms.get(term) else {
                return Ok(None);
            };
            // collect related terms of the original term.
            for term_id in fuzz_term_ids {
                let doc_ids = self.term_reader.get_doc_ids(*term_id)?;
                all_doc_ids.bitor_assign(doc_ids);
            }
            if !all_doc_ids.is_empty() {
                if self.term_reader.has_score {
                    let query_key = format!("{:?}", fuzzy_term_query);
                    self.query_fuzzy_map.insert(query_key, all_doc_ids.clone());
                }
                Ok(Some(all_doc_ids))
            } else {
                Ok(None)
            }
        } else if let Some(boost_query) = query.downcast_ref::<BoostQuery>() {
            self.collect_matched_doc_ids(
                boost_query.underlying_query(),
                prefix_terms,
                fuzziness_terms,
            )
        } else if let Some(const_query) = query.downcast_ref::<ConstScoreQuery>() {
            self.collect_matched_doc_ids(
                const_query.underlying_query(),
                prefix_terms,
                fuzziness_terms,
            )
        } else if let Some(_empty_query) = query.downcast_ref::<EmptyQuery>() {
            Ok(None)
        } else if let Some(_all_query) = query.downcast_ref::<AllQuery>() {
            let all_doc_ids = RoaringTreemap::from_iter(0..self.term_reader.row_count);
            Ok(Some(all_doc_ids))
        } else {
            Err(ErrorCode::TantivyError(format!(
                "inverted index unsupported query `{:?}`",
                query
            )))
        }
    }

    fn calculate_phrase_scores(
        &mut self,
        query_key: String,
        phrase_terms: Vec<Term>,
        doc_ids: &RoaringTreemap,
        boost: Option<f32>,
    ) -> Result<Vec<F32>> {
        let mut phrase_counts_map = self
            .query_phrase_counts_map
            .remove(&query_key)
            .unwrap_or_default();
        if let Some(term_id) = self.term_reader.term_id(&phrase_terms[0]) {
            if let Some(field_id) = self.term_reader.field_id(term_id) {
                let mut bm25_weight = Bm25Weight::for_terms(&self.term_reader, &phrase_terms)?;
                if let Some(boost) = boost {
                    // increase weight by multiply a optional boost factor
                    bm25_weight = bm25_weight.boost_by(boost);
                }
                let mut scores = Vec::with_capacity(doc_ids.len() as usize);
                for doc_id in doc_ids {
                    let fieldnorm_id = self.term_reader.fieldnorm_id(*field_id, doc_id);
                    let phrase_count = phrase_counts_map.remove(&doc_id).unwrap_or_default();

                    let score = bm25_weight.score(fieldnorm_id, phrase_count as u32);
                    scores.push(score.into());
                }
                return Ok(scores);
            }
        }
        let scores = vec![F32::from(0_f32); doc_ids.len() as usize];
        Ok(scores)
    }

    // calculate the score of matched doc ids.
    pub fn calculate_scores(
        &mut self,
        query: Box<dyn Query>,
        doc_ids: &RoaringTreemap,
        boost: Option<f32>,
    ) -> Result<Vec<F32>> {
        if let Some(term_query) = query.downcast_ref::<TermQuery>() {
            let term = term_query.term();
            if let Some(term_id) = self.term_reader.term_id(term) {
                if let Some(field_id) = self.term_reader.field_id(term_id) {
                    let mut bm25_weight =
                        Bm25Weight::for_terms(&self.term_reader, &[term.clone()])?;
                    if let Some(boost) = boost {
                        // increase weight by multiply a optional boost factor
                        bm25_weight = bm25_weight.boost_by(boost);
                    }
                    let mut scores = Vec::with_capacity(doc_ids.len() as usize);
                    for doc_id in doc_ids {
                        let fieldnorm_id = self.term_reader.fieldnorm_id(*field_id, doc_id);
                        let term_freq = self.term_reader.term_freq(*term_id, doc_id).unwrap_or(&0);

                        let score = bm25_weight.score(fieldnorm_id, *term_freq);
                        scores.push(score.into());
                    }
                    return Ok(scores);
                }
            }
            let scores = vec![F32::from(0_f32); doc_ids.len() as usize];
            Ok(scores)
        } else if let Some(bool_query) = query.downcast_ref::<BooleanQuery>() {
            let mut combine_scores = vec![F32::from(0_f32); doc_ids.len() as usize];
            for (occur, sub_query) in bool_query.clauses() {
                // ignore `MustNot` sub queries
                if occur == &Occur::MustNot {
                    continue;
                }
                let scores = self.calculate_scores(sub_query.box_clone(), doc_ids, boost)?;
                for i in 0..combine_scores.len() {
                    // To simplify the calculation, add the score of the sub query together as the combine score.
                    unsafe {
                        let combine_score = combine_scores.get_unchecked_mut(i);
                        if let Some(score) = scores.get(i) {
                            *combine_score += score;
                        }
                    }
                }
            }
            Ok(combine_scores)
        } else if let Some(phrase_query) = query.downcast_ref::<PhraseQuery>() {
            let query_key = format!("{:?}", phrase_query);
            let phrase_terms = phrase_query.phrase_terms();
            self.calculate_phrase_scores(query_key, phrase_terms, doc_ids, boost)
        } else if let Some(phrase_prefix_query) = query.downcast_ref::<PhrasePrefixQuery>() {
            let query_key = format!("{:?}", phrase_prefix_query);
            let phrase_terms = phrase_prefix_query.phrase_terms();
            self.calculate_phrase_scores(query_key, phrase_terms, doc_ids, boost)
        } else if let Some(fuzzy_term_query) = query.downcast_ref::<FuzzyTermQuery>() {
            let query_key = format!("{:?}", fuzzy_term_query);
            let fuzzy_doc_ids = self.query_fuzzy_map.remove(&query_key).unwrap_or_default();

            let mut scores = Vec::with_capacity(doc_ids.len() as usize);
            for doc_id in doc_ids {
                if fuzzy_doc_ids.contains(doc_id) {
                    scores.push(F32::from(1_f32));
                } else {
                    scores.push(F32::from(0_f32));
                }
            }
            Ok(scores)
        } else if let Some(boost_query) = query.downcast_ref::<BoostQuery>() {
            let boost = boost_query.get_boost();
            self.calculate_scores(boost_query.underlying_query(), doc_ids, Some(boost))
        } else if let Some(const_query) = query.downcast_ref::<ConstScoreQuery>() {
            let score = const_query.get_const_score();
            let scores = vec![F32::from(score); doc_ids.len() as usize];
            Ok(scores)
        } else if let Some(_all_query) = query.downcast_ref::<AllQuery>() {
            let scores = vec![F32::from(1_f32); doc_ids.len() as usize];
            Ok(scores)
        } else {
            let scores = vec![F32::from(0_f32); doc_ids.len() as usize];
            Ok(scores)
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct InvertedIndexMeta {
    pub version: usize,
    pub columns: Vec<(String, SingleColumnMeta)>,
}

impl TryFrom<FileMetaData> for InvertedIndexMeta {
    type Error = ErrorCode;

    fn try_from(mut meta: FileMetaData) -> std::result::Result<Self, Self::Error> {
        let rg = meta.row_groups.remove(0);
        let mut col_metas = Vec::with_capacity(rg.columns.len());
        for x in &rg.columns {
            match &x.meta_data {
                Some(chunk_meta) => {
                    let col_start =
                        if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                            dict_page_offset
                        } else {
                            chunk_meta.data_page_offset
                        };
                    let col_len = chunk_meta.total_compressed_size;
                    assert!(
                        col_start >= 0 && col_len >= 0,
                        "column start and length should not be negative"
                    );
                    let num_values = chunk_meta.num_values as u64;
                    let res = SingleColumnMeta {
                        offset: col_start as u64,
                        len: col_len as u64,
                        num_values,
                    };
                    let column_name = chunk_meta.path_in_schema[0].to_owned();
                    col_metas.push((column_name, res));
                }
                None => {
                    panic!(
                        "expecting chunk meta data while converting ThriftFileMetaData to InvertedIndexMeta"
                    )
                }
            }
        }
        col_metas.shrink_to_fit();

        Ok(Self {
            version: 3,
            columns: col_metas,
        })
    }
}

#[derive(Clone, Debug)]
pub struct InvertedIndexFile {
    pub name: String,
    pub data: OwnedBytes,
}

impl InvertedIndexFile {
    pub fn create(name: String, data: Vec<u8>) -> Self {
        let data = OwnedBytes::new(data);
        Self { name, data }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerializableInvertedIndexFile {
    name: String,
    data: Vec<u8>,
}

impl TryFrom<&InvertedIndexFile> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &InvertedIndexFile) -> std::result::Result<Self, Self::Error> {
        let serializable = SerializableInvertedIndexFile {
            name: value.name.clone(),
            data: value.data.as_slice().to_vec(),
        };
        bincode::serde::encode_to_vec(&serializable, bincode::config::standard()).map_err(|e| {
            ErrorCode::StorageOther(format!("failed to encode inverted index file {:?}", e))
        })
    }
}

impl TryFrom<Bytes> for InvertedIndexFile {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::serde::decode_from_slice(value.as_ref(), bincode::config::standard())
            .map(|(v, len)| {
                assert_eq!(len, value.len());
                v
            })
            .map(|v: SerializableInvertedIndexFile| InvertedIndexFile {
                name: v.name,
                data: OwnedBytes::new(v.data),
            })
            .map_err(|e| {
                ErrorCode::StorageOther(format!("failed to decode inverted index file {:?}", e))
            })
    }
}

impl TryFrom<&InvertedIndexMeta> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &InvertedIndexMeta) -> std::result::Result<Self, Self::Error> {
        bincode::serde::encode_to_vec(value, bincode::config::standard()).map_err(|e| {
            ErrorCode::StorageOther(format!("failed to encode inverted index meta {:?}", e))
        })
    }
}

impl TryFrom<Bytes> for InvertedIndexMeta {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::serde::decode_from_slice(value.as_ref(), bincode::config::standard())
            .map(|(v, len)| {
                assert_eq!(len, value.len());
                v
            })
            .map_err(|e| {
                ErrorCode::StorageOther(format!("failed to decode inverted index meta {:?}", e))
            })
    }
}

/// The Writer just writes a buffer.
struct VecWriter {
    path: PathBuf,
    data: Cursor<Vec<u8>>,
    is_flushed: bool,
}

impl VecWriter {
    fn new(path_buf: PathBuf) -> VecWriter {
        VecWriter {
            path: path_buf,
            data: Cursor::new(Vec::new()),
            is_flushed: true,
        }
    }
}

impl Drop for VecWriter {
    fn drop(&mut self) {
        if !self.is_flushed {
            warn!(
                "You forgot to flush {:?} before its writer got Drop. Do not rely on drop. This \
                 also occurs when the indexer crashed, so you may want to check the logs for the \
                 root cause.",
                self.path
            )
        }
    }
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.is_flushed = false;
        self.data.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.is_flushed = true;
        Ok(())
    }
}

impl TerminatingWrite for VecWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

// InvertedIndexDirectory holds all indexed data for tantivy queries to search for relevant data.
// The data is read-only, write and delete operations will be ignored and return success directly.
#[derive(Clone, Debug)]
pub struct InvertedIndexDirectory {
    meta_path: PathBuf,
    managed_path: PathBuf,

    fast_data: OwnedBytes,
    store_data: OwnedBytes,
    fieldnorm_data: OwnedBytes,
    pos_data: OwnedBytes,
    idx_data: OwnedBytes,
    term_data: OwnedBytes,
    meta_data: OwnedBytes,
    managed_data: OwnedBytes,
}

impl InvertedIndexDirectory {
    pub fn try_create(files: Vec<Arc<InvertedIndexFile>>) -> Result<Self> {
        let mut file_map = BTreeMap::<String, OwnedBytes>::new();

        for file in files.into_iter() {
            let file = Arc::unwrap_or_clone(file.clone());
            if file.data.is_empty() {
                continue;
            }
            file_map.insert(file.name, file.data);
        }

        let fast_data = file_map.remove("fast").unwrap();
        let store_data = file_map.remove("store").unwrap();
        let fieldnorm_data = file_map.remove("fieldnorm").unwrap();
        let pos_data = file_map.remove("pos").unwrap();
        let idx_data = file_map.remove("idx").unwrap();
        let term_data = file_map.remove("term").unwrap();
        let meta_data = file_map.remove("meta.json").unwrap();
        let managed_data = file_map.remove(".managed.json").unwrap();

        let meta_path = PathBuf::from("meta.json");
        let managed_path = PathBuf::from(".managed.json");

        Ok(Self {
            meta_path,
            managed_path,

            fast_data,
            store_data,
            fieldnorm_data,
            pos_data,
            idx_data,
            term_data,
            meta_data,
            managed_data,
        })
    }

    pub fn size(&self) -> usize {
        self.fast_data.len()
            + self.store_data.len()
            + self.fieldnorm_data.len()
            + self.pos_data.len()
            + self.idx_data.len()
            + self.term_data.len()
            + self.meta_data.len()
            + self.managed_data.len()
            + self.meta_path.capacity()
            + self.managed_path.capacity()
    }
}

impl Directory for InvertedIndexDirectory {
    fn get_file_handle(&self, path: &Path) -> result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> result::Result<FileSlice, OpenReadError> {
        if path == self.meta_path.as_path() {
            return Ok(FileSlice::new(Arc::new(self.meta_data.clone())));
        } else if path == self.managed_path.as_path() {
            return Ok(FileSlice::new(Arc::new(self.managed_data.clone())));
        }

        if let Some(ext) = path.extension() {
            let bytes = match ext.to_str() {
                Some("term") => self.term_data.clone(),
                Some("idx") => self.idx_data.clone(),
                Some("pos") => self.pos_data.clone(),
                Some("fieldnorm") => self.fieldnorm_data.clone(),
                Some("store") => self.store_data.clone(),
                Some("fast") => self.fast_data.clone(),
                _ => {
                    return Err(OpenReadError::FileDoesNotExist(PathBuf::from(path)));
                }
            };
            Ok(FileSlice::new(Arc::new(bytes)))
        } else {
            Err(OpenReadError::FileDoesNotExist(PathBuf::from(path)))
        }
    }

    fn delete(&self, _path: &Path) -> result::Result<(), DeleteError> {
        Ok(())
    }

    fn exists(&self, path: &Path) -> result::Result<bool, OpenReadError> {
        if path == self.meta_path.as_path() || path == self.managed_path.as_path() {
            return Ok(true);
        }
        if let Some(ext) = path.extension() {
            match ext.to_str() {
                Some("term") => Ok(true),
                Some("idx") => Ok(true),
                Some("pos") => Ok(true),
                Some("fieldnorm") => Ok(true),
                Some("store") => Ok(true),
                Some("fast") => Ok(true),
                _ => Ok(false),
            }
        } else {
            Ok(false)
        }
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        let path_buf = PathBuf::from(path);
        let vec_writer = VecWriter::new(path_buf);
        Ok(BufWriter::new(Box::new(vec_writer)))
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        let bytes =
            self.open_read(path)?
                .read_bytes()
                .map_err(|io_error| OpenReadError::IoError {
                    io_error: Arc::new(io_error),
                    filepath: path.to_path_buf(),
                })?;
        Ok(bytes.as_slice().to_owned())
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        let watch_handle = WatchHandle::new(Arc::new(watch_callback));
        Ok(watch_handle)
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
}

impl Versioned<0> for InvertedIndexFile {}

#[allow(dead_code)]
pub enum InvertedIndexFileVersion {
    V0(PhantomData<InvertedIndexFile>),
}

impl TryFrom<u64> for InvertedIndexFileVersion {
    type Error = ErrorCode;
    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(InvertedIndexFileVersion::V0(testify_version::<_, 0>(
                PhantomData,
            ))),
            _ => Err(ErrorCode::Internal(format!(
                "unknown inverted index file version {value}, versions supported: 0"
            ))),
        }
    }
}
