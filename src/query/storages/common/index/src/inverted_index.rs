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
use std::collections::VecDeque;
use std::io;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use std::result;
use std::sync::Arc;

use crc32fast::Hasher;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_storages_common_table_meta::meta::testify_version;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::Versioned;
use levenshtein_automata::Distance;
use levenshtein_automata::LevenshteinAutomatonBuilder;
use levenshtein_automata::DFA;
use log::warn;
use parquet::format::FileMetaData;
use tantivy::directory::error::DeleteError;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::error::OpenWriteError;
use tantivy::directory::AntiCallToken;
use tantivy::directory::FileHandle;
use tantivy::directory::FileSlice;
use tantivy::directory::OwnedBytes;
use tantivy::directory::TerminatingWrite;
use tantivy::directory::WatchCallback;
use tantivy::directory::WatchHandle;
use tantivy::directory::WritePtr;
use tantivy::positions::PositionReader;
use tantivy::postings::TermInfo;
use tantivy::query::BooleanQuery;
use tantivy::query::FuzzyTermQuery;
use tantivy::query::Occur;
use tantivy::query::PhraseQuery;
use tantivy::query::Query;
use tantivy::query::QueryClone;
use tantivy::query::TermQuery;
use tantivy::Directory;
use tantivy::Term;
use tantivy_common::BinarySerializable;
use tantivy_common::HasLen;
use tantivy_common::VInt;
use tantivy_fst::Automaton;
use tantivy_fst::IntoStreamer;
use tantivy_fst::Streamer;

// tantivy version is used to generate the footer data

// Index major version.
const INDEX_MAJOR_VERSION: u32 = 0;
// Index minor version.
const INDEX_MINOR_VERSION: u32 = 22;
// Index patch version.
const INDEX_PATCH_VERSION: u32 = 0;
// Index format version.
const INDEX_FORMAT_VERSION: u32 = 6;

// The magic byte of the footer to identify corruption
// or an old version of the footer.
const FOOTER_MAGIC_NUMBER: u32 = 1337;

type CrcHashU32 = u32;

/// Structure version for the index.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Version {
    major: u32,
    minor: u32,
    patch: u32,
    index_format_version: u32,
}

/// A Footer is appended every part of data, like tantivy file.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Footer {
    version: Version,
    crc: CrcHashU32,
}

impl Footer {
    fn new(crc: CrcHashU32) -> Self {
        let version = Version {
            major: INDEX_MAJOR_VERSION,
            minor: INDEX_MINOR_VERSION,
            patch: INDEX_PATCH_VERSION,
            index_format_version: INDEX_FORMAT_VERSION,
        };
        Footer { version, crc }
    }

    fn append_footer<W: std::io::Write>(&self, write: &mut W) -> Result<()> {
        let footer_payload_len = write.write(serde_json::to_string(&self)?.as_ref())?;
        BinarySerializable::serialize(&(footer_payload_len as u32), write)?;
        BinarySerializable::serialize(&FOOTER_MAGIC_NUMBER, write)?;
        Ok(())
    }
}

fn extract_footer(data: FileSlice) -> Result<(usize, Vec<usize>)> {
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

    let mut offsets = vec![];
    let mut offset = 0;
    for _ in 0..num_fields {
        offset += VInt::deserialize(&mut footer_buffer)?.0 as usize;
        let _file_addr = FileAddr::deserialize(&mut footer_buffer)?;
        offsets.push(offset);
    }
    offsets.push(footer_start);

    Ok((num_fields, offsets))
}

// Extract fsts from term dict file.
pub fn extract_fsts(
    data: FileSlice,
    fields: &mut Vec<TableField>,
    values: &mut Vec<Scalar>,
) -> Result<()> {
    let (num_fields, offsets) = extract_footer(data.clone())?;

    // The following code is copied from tantivy `TermDictionary::open` function.
    // extract fst data from field.
    for i in 0..num_fields {
        let start_offset = offsets[i];
        let end_offset = offsets[i + 1];

        let field_slice = data.slice(start_offset..end_offset);

        let (main_slice, footer_len_slice) = field_slice.split_from_end(16);
        let mut footer_len_bytes = footer_len_slice.read_bytes()?;
        let footer_size = u64::deserialize(&mut footer_len_bytes)?;

        let (fst_file_slice, term_dict_file_slice) =
            main_slice.split_from_end(footer_size as usize);

        let fst_field_name = format!("fst-{}", i);
        let fst_field = TableField::new(&fst_field_name, TableDataType::Binary);
        fields.push(fst_field);

        let fst_bytes = fst_file_slice.read_bytes()?;
        values.push(Scalar::Binary(fst_bytes.as_slice().to_vec()));

        let term_dict_field_name = format!("term-{}", i);
        let term_dict_field = TableField::new(&term_dict_field_name, TableDataType::Binary);
        fields.push(term_dict_field);

        let term_dict_bytes = term_dict_file_slice.read_bytes()?;
        values.push(Scalar::Binary(term_dict_bytes.as_slice().to_vec()));
    }

    Ok(())
}

pub fn extract_component_fields(
    name: &str,
    data: FileSlice,
    fields: &mut Vec<TableField>,
    values: &mut Vec<Scalar>,
) -> Result<()> {
    let (num_fields, offsets) = extract_footer(data.clone())?;

    for i in 0..num_fields {
        let start_offset = offsets[i];
        let end_offset = offsets[i + 1];

        let field_slice = data.slice(start_offset..end_offset);

        let fst_field_name = format!("{}-{}", name, i);
        let fst_field = TableField::new(&fst_field_name, TableDataType::Binary);
        fields.push(fst_field);

        let idx_bytes = field_slice.read_bytes()?;
        values.push(Scalar::Binary(idx_bytes.as_slice().to_vec()));
    }

    Ok(())
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

#[derive(
    Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
struct Field(u32);

impl Field {
    /// Create a new field object for the given FieldId.
    const fn from_field_id(field_id: u32) -> Field {
        Field(field_id)
    }

    /// Returns a u32 identifying uniquely a field within a schema.
    #[allow(dead_code)]
    const fn field_id(self) -> u32 {
        self.0
    }
}

impl BinarySerializable for Field {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.0.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Field> {
        u32::deserialize(reader).map(Field)
    }
}

#[derive(Eq, PartialEq, Hash, Copy, Ord, PartialOrd, Clone, Debug)]
struct FileAddr {
    field: Field,
    idx: usize,
}

impl FileAddr {
    fn new(field: Field, idx: usize) -> FileAddr {
        FileAddr { field, idx }
    }
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

// Build empty position data to be used when there are no phrase terms in the query.
// This can reduce data reading and speed up the query
fn build_empty_position_data(field_nums: usize) -> Result<OwnedBytes> {
    let offsets: Vec<_> = (0..field_nums)
        .map(|i| {
            let field = Field::from_field_id(i as u32);
            let file_addr = FileAddr::new(field, 0);
            (file_addr, 0)
        })
        .collect();

    let mut buf = Vec::new();
    VInt(offsets.len() as u64).serialize(&mut buf)?;

    let mut prev_offset = 0;
    for (file_addr, offset) in offsets {
        VInt(offset - prev_offset).serialize(&mut buf)?;
        file_addr.serialize(&mut buf)?;
        prev_offset = offset;
    }

    let footer_len = buf.len() as u32;
    footer_len.serialize(&mut buf)?;

    let mut footer = build_tantivy_footer(&buf)?;
    buf.append(&mut footer);

    Ok(OwnedBytes::new(buf))
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

// Term value contains values associated with a Term
// used to match query and collect matched doc ids.
#[derive(Clone)]
pub struct TermValue {
    // term info
    pub term_info: TermInfo,
    // term matched doc ids
    pub doc_ids: Vec<u32>,
    // term frequences for each doc
    pub term_freqs: Vec<u32>,
    // position reader is used to read positions in doc for phrase query
    pub position_reader: Option<PositionReader>,
}

// Check if fst contains terms in query.
// If not, we can skip read other parts of inverted index.
pub fn check_term_fsts_match(
    query: Box<dyn Query>,
    fst_maps: &HashMap<usize, tantivy_fst::Map<OwnedBytes>>,
    fuzziness: &Option<u8>,
    matched_terms: &mut HashMap<Term, u64>,
    fuzziness_terms: &mut HashMap<Term, Vec<Term>>,
) -> bool {
    if let Some(term_query) = query.downcast_ref::<TermQuery>() {
        let term = term_query.term();
        let field = term.field();
        let field_id = field.field_id() as usize;
        if let Some(fst_map) = fst_maps.get(&field_id) {
            if let Some(idx) = fst_map.get(term.serialized_value_bytes()) {
                matched_terms.insert(term.clone(), idx);
                return true;
            }
        }
        false
    } else if let Some(bool_query) = query.downcast_ref::<BooleanQuery>() {
        let mut matched_num = 0;
        for (occur, sub_query) in bool_query.clauses() {
            let matched = check_term_fsts_match(
                sub_query.box_clone(),
                fst_maps,
                fuzziness,
                matched_terms,
                fuzziness_terms,
            );
            if matched {
                matched_num += 1;
            }
            match occur {
                Occur::Should => {}
                Occur::Must => {
                    if !matched {
                        return false;
                    }
                }
                Occur::MustNot => {}
            }
        }
        matched_num > 0
    } else if let Some(phrase_query) = query.downcast_ref::<PhraseQuery>() {
        // PhraseQuery must match all terms.
        let field = phrase_query.field();
        let field_id = field.field_id() as usize;
        if let Some(fst_map) = fst_maps.get(&field_id) {
            let mut matched_all = true;
            for term in phrase_query.phrase_terms() {
                let matched = if let Some(idx) = fst_map.get(term.serialized_value_bytes()) {
                    matched_terms.insert(term.clone(), idx);
                    true
                } else {
                    false
                };
                if !matched {
                    matched_all = false;
                    break;
                }
            }
            matched_all
        } else {
            false
        }
    } else if let Some(fuzzy_term_query) = query.downcast_ref::<FuzzyTermQuery>() {
        // FuzzyTermQuery match terms by levenshtein distance.
        let fuzziness = fuzziness.unwrap();

        let term = fuzzy_term_query.term();
        let field = term.field();
        let field_id = field.field_id() as usize;
        if let Some(fst_map) = fst_maps.get(&field_id) {
            // build levenshtein automaton
            let lev_automaton_builder = LevenshteinAutomatonBuilder::new(fuzziness, true);
            let term_str = String::from_utf8_lossy(term.serialized_value_bytes());
            let automaton = DfaWrapper(lev_automaton_builder.build_dfa(&term_str));

            let mut fuzz_term_values = vec![];
            let mut stream = fst_map.search(automaton).into_stream();
            while let Some((key, idx)) = stream.next() {
                let key_str = unsafe { std::str::from_utf8_unchecked(key) };
                let fuzz_term = Term::from_field_text(field, key_str);
                matched_terms.insert(fuzz_term.clone(), idx);
                fuzz_term_values.push(fuzz_term);
            }
            let matched = !fuzz_term_values.is_empty();
            fuzziness_terms.insert(term.clone(), fuzz_term_values);
            matched
        } else {
            false
        }
    } else {
        // TODO: handle other Query types
        let mut matched = false;
        query.query_terms(&mut |term, _| {
            let field = term.field();
            let field_id = field.field_id() as usize;
            if let Some(fst_map) = fst_maps.get(&field_id) {
                if let Some(idx) = fst_map.get(term.serialized_value_bytes()) {
                    matched_terms.insert(term.clone(), idx);
                    matched = true;
                }
            }
        });

        matched
    }
}

// collect matched rows by term value
pub fn collect_matched_rows(
    query: Box<dyn Query>,
    row_count: u32,
    fuzziness_terms: &HashMap<Term, Vec<Term>>,
    term_values: &mut HashMap<Term, TermValue>,
) -> Vec<u32> {
    if let Some(term_query) = query.downcast_ref::<TermQuery>() {
        let term = term_query.term();
        if let Some(term_value) = term_values.get(term) {
            term_value.doc_ids.clone()
        } else {
            vec![]
        }
    } else if let Some(bool_query) = query.downcast_ref::<BooleanQuery>() {
        let mut should_doc_ids_opt = None;
        let mut must_doc_ids_opt = None;
        let mut must_not_doc_ids_opt = None;
        for (occur, sub_query) in bool_query.clauses() {
            let doc_ids = collect_matched_rows(
                sub_query.box_clone(),
                row_count,
                fuzziness_terms,
                term_values,
            );
            let doc_id_set = HashSet::from_iter(doc_ids.into_iter());
            match occur {
                Occur::Should => {
                    if should_doc_ids_opt.is_none() {
                        should_doc_ids_opt = Some(doc_id_set);
                    } else {
                        let should_doc_ids = should_doc_ids_opt.unwrap();
                        should_doc_ids_opt =
                            Some(should_doc_ids.union(&doc_id_set).copied().collect())
                    }
                }
                Occur::Must => {
                    if must_doc_ids_opt.is_none() {
                        must_doc_ids_opt = Some(doc_id_set);
                    } else {
                        let must_doc_ids = must_doc_ids_opt.unwrap();
                        must_doc_ids_opt =
                            Some(must_doc_ids.intersection(&doc_id_set).copied().collect())
                    }
                }
                Occur::MustNot => {
                    if must_not_doc_ids_opt.is_none() {
                        must_not_doc_ids_opt = Some(doc_id_set);
                    } else {
                        let must_not_doc_ids = must_not_doc_ids_opt.unwrap();
                        must_not_doc_ids_opt =
                            Some(must_not_doc_ids.union(&doc_id_set).copied().collect())
                    }
                }
            }
        }

        let doc_ids = if let Some(mut should_doc_ids) = should_doc_ids_opt {
            if let Some(must_doc_ids) = must_doc_ids_opt {
                should_doc_ids = should_doc_ids
                    .intersection(&must_doc_ids)
                    .copied()
                    .collect()
            }
            if let Some(must_not_doc_ids) = must_not_doc_ids_opt {
                should_doc_ids = should_doc_ids
                    .difference(&must_not_doc_ids)
                    .copied()
                    .collect()
            }
            should_doc_ids
        } else if let Some(mut must_doc_ids) = must_doc_ids_opt {
            if let Some(must_not_doc_ids) = must_not_doc_ids_opt {
                must_doc_ids = must_doc_ids
                    .difference(&must_not_doc_ids)
                    .copied()
                    .collect()
            }
            must_doc_ids
        } else if let Some(must_not_doc_ids) = must_not_doc_ids_opt {
            let all_doc_ids = HashSet::from_iter(0..row_count);
            let doc_ids = all_doc_ids.difference(&must_not_doc_ids).copied().collect();
            doc_ids
        } else {
            HashSet::new()
        };

        let mut doc_ids = Vec::from_iter(doc_ids);
        doc_ids.sort();
        doc_ids
    } else if let Some(phrase_query) = query.downcast_ref::<PhraseQuery>() {
        let mut union_doc_ids = HashSet::new();
        let mut intersection_doc_ids_opt = None;

        for term in phrase_query.phrase_terms() {
            if let Some(term_value) = term_values.get(&term) {
                let doc_id_set = HashSet::from_iter(term_value.doc_ids.clone());
                union_doc_ids = union_doc_ids.union(&doc_id_set).copied().collect();
                if intersection_doc_ids_opt.is_none() {
                    intersection_doc_ids_opt = Some(doc_id_set);
                } else {
                    let intersection_doc_ids = intersection_doc_ids_opt.unwrap();
                    intersection_doc_ids_opt = Some(
                        intersection_doc_ids
                            .intersection(&doc_id_set)
                            .copied()
                            .collect(),
                    );
                }
            }
        }

        let intersection_doc_ids = intersection_doc_ids_opt.unwrap_or_default();
        if intersection_doc_ids.is_empty() {
            return vec![];
        }
        let mut union_doc_ids = Vec::from_iter(union_doc_ids);
        union_doc_ids.sort();

        // check each docs
        let mut matched_doc_ids = vec![];
        for doc_id in union_doc_ids {
            if !intersection_doc_ids.contains(&doc_id) {
                continue;
            }

            let mut term_pos_map = HashMap::new();
            for term in phrase_query.phrase_terms() {
                let mut offset = 0;
                let mut term_freq = 0;
                if let Some(term_value) = term_values.get_mut(&term) {
                    for i in 0..term_value.doc_ids.len() {
                        if term_value.doc_ids[i] < doc_id {
                            offset += term_value.term_freqs[i] as u64;
                        } else {
                            term_freq = term_value.term_freqs[i] as usize;
                            break;
                        }
                    }
                    // collect positions in the docs
                    if let Some(position_reader) = term_value.position_reader.as_mut() {
                        let mut pos_output = vec![0; term_freq];
                        position_reader.read(offset, &mut pos_output[..]);
                        for i in 1..pos_output.len() {
                            pos_output[i] += pos_output[i - 1];
                        }
                        let positions = VecDeque::from_iter(pos_output);
                        term_pos_map.insert(term.clone(), positions);
                    }
                }
            }

            let mut is_first = true;
            let mut distance = 0;
            let mut matched = true;
            let mut last_position = 0;
            for (query_position, term) in phrase_query.phrase_terms_with_offsets() {
                if let Some(positions) = term_pos_map.get_mut(&term) {
                    let mut find_position = false;
                    while let Some(doc_position) = positions.pop_front() {
                        // skip previous positions.
                        if doc_position < last_position {
                            continue;
                        }
                        last_position = doc_position;
                        let doc_distance = doc_position - (query_position as u32);
                        if is_first {
                            is_first = false;
                            distance = doc_distance;
                        } else {
                            // distance must same as first term.
                            if doc_distance != distance {
                                matched = false;
                            }
                        }
                        find_position = true;
                        break;
                    }
                    if !find_position {
                        matched = false;
                    }
                } else {
                    matched = false;
                }
                if !matched {
                    break;
                }
            }
            if matched {
                matched_doc_ids.push(doc_id);
            }
        }
        matched_doc_ids
    } else if let Some(fuzzy_term_query) = query.downcast_ref::<FuzzyTermQuery>() {
        let mut fuzz_doc_ids = HashSet::new();
        let term = fuzzy_term_query.term();

        // collect related terms of the original term.
        if let Some(related_terms) = fuzziness_terms.get(term) {
            for term in related_terms {
                if let Some(term_value) = term_values.get(term) {
                    let doc_id_set: HashSet<u32> = HashSet::from_iter(term_value.doc_ids.clone());
                    fuzz_doc_ids = fuzz_doc_ids.union(&doc_id_set).copied().collect();
                }
            }
            let mut doc_ids = Vec::from_iter(fuzz_doc_ids);
            doc_ids.sort();
            doc_ids
        } else {
            vec![]
        }
    } else {
        let mut union_doc_ids = HashSet::new();
        query.query_terms(&mut |term, _| {
            if let Some(term_value) = term_values.get(term) {
                let doc_id_set: HashSet<u32> = HashSet::from_iter(term_value.doc_ids.clone());
                union_doc_ids = union_doc_ids.union(&doc_id_set).copied().collect();
            }
        });

        let mut doc_ids = Vec::from_iter(union_doc_ids);
        doc_ids.sort();
        doc_ids
    }
}

#[derive(Clone)]
pub struct InvertedIndexMeta {
    pub columns: Vec<(String, SingleColumnMeta)>,
}

impl TryFrom<FileMetaData> for InvertedIndexMeta {
    type Error = databend_common_exception::ErrorCode;

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
                        offset: (col_start + 23) as u64,
                        len: (col_len - 23) as u64,
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
        Ok(Self { columns: col_metas })
    }
}

#[derive(Clone, Debug)]
pub struct InvertedIndexFile {
    pub name: String,
    pub data: OwnedBytes,
}

impl InvertedIndexFile {
    pub fn try_create(name: String, data: Vec<u8>) -> Result<Self> {
        let data = OwnedBytes::new(data);
        Ok(Self { name, data })
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
    pub fn try_create(field_nums: usize, files: Vec<Arc<InvertedIndexFile>>) -> Result<Self> {
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
        // If there are no phrase terms in the query,
        // we can use empty position data instead.
        let pos_data = match file_map.remove("pos") {
            Some(pos_data) => pos_data,
            None => build_empty_position_data(field_nums)?,
        };
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
