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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_search_milliseconds;
use databend_storages_common_index::check_term_fsts_match;
use databend_storages_common_index::collect_matched_rows;
use databend_storages_common_index::TermValue;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use futures_util::future::try_join_all;
use opendal::Operator;
use tantivy::collector::DocSetCollector;
use tantivy::collector::TopDocs;
use tantivy::directory::FileSlice;
use tantivy::directory::OwnedBytes;
use tantivy::positions::PositionReader;
use tantivy::postings::BlockSegmentPostings;
use tantivy::query::Query;
use tantivy::query::QueryClone;
use tantivy::schema::IndexRecordOption;
use tantivy::termdict::TermInfoStore;
use tantivy::tokenizer::TokenizerManager;
use tantivy::Index;
use tantivy_fst::raw::Fst;

use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_directory;
use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_file;
use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_meta;
use crate::io::read::inverted_index::inverted_index_loader::InvertedIndexFileReader;

#[derive(Clone)]
pub struct InvertedIndexReader {
    dal: Operator,
}

impl InvertedIndexReader {
    pub fn create(dal: Operator) -> Self {
        Self { dal }
    }

    // Filter the rows and scores in the block that can match the query text,
    // if there is no row that can match, this block can be pruned.
    #[allow(clippy::type_complexity)]
    #[allow(clippy::too_many_arguments)]
    pub async fn do_filter(
        self,
        field_nums: usize,
        need_position: bool,
        has_score: bool,
        query: Box<dyn Query>,
        field_ids: &HashSet<usize>,
        index_record: &IndexRecordOption,
        fuzziness: &Option<u8>,
        tokenizer_manager: TokenizerManager,
        row_count: u32,
        index_loc: &str,
    ) -> Result<Option<Vec<(usize, Option<F32>)>>> {
        let start = Instant::now();

        let matched_rows = self
            .search(
                index_loc,
                query,
                field_ids,
                field_nums,
                need_position,
                has_score,
                index_record,
                fuzziness,
                tokenizer_manager,
                row_count,
            )
            .await?;

        // Perf.
        {
            metrics_inc_block_inverted_index_search_milliseconds(start.elapsed().as_millis() as u64);
        }

        Ok(matched_rows)
    }

    async fn read_column_data<'a>(
        &self,
        index_path: &'a str,
        name: &str,
        field_ids: &HashSet<usize>,
        inverted_index_meta_map: &HashMap<String, SingleColumnMeta>,
    ) -> Result<HashMap<usize, OwnedBytes>> {
        let mut col_metas = vec![];
        let mut col_field_map = HashMap::new();
        for field_id in field_ids {
            let col_name = format!("{}-{}", name, field_id);
            let col_meta = inverted_index_meta_map.get(&col_name).unwrap();

            col_metas.push((col_name.clone(), col_meta));
            col_field_map.insert(col_name, *field_id);
        }

        let futs = col_metas
            .iter()
            .map(|(name, col_meta)| load_inverted_index_file(name, col_meta, index_path, &self.dal))
            .collect::<Vec<_>>();

        let col_files = try_join_all(futs)
            .await?
            .into_iter()
            .map(|f| {
                let field_id = col_field_map.get(&f.name).unwrap();
                (*field_id, f.data.clone())
            })
            .collect::<HashMap<_, _>>();

        Ok(col_files)
    }

    // first version search function, using tantivy searcher.
    async fn search_v0<'a>(
        &self,
        index_path: &'a str,
        query: Box<dyn Query>,
        field_nums: usize,
        has_score: bool,
        tokenizer_manager: TokenizerManager,
        row_count: u32,
    ) -> Result<Option<Vec<(usize, Option<F32>)>>> {
        let directory =
            load_inverted_index_directory(self.dal.clone(), field_nums, index_path).await?;

        let mut index = Index::open(directory)?;
        index.set_tokenizers(tokenizer_manager);
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let matched_rows = if has_score {
            let collector = TopDocs::with_limit(row_count as usize);
            let docs = searcher.search(&query, &collector)?;

            let mut matched_rows = Vec::with_capacity(docs.len());
            for (score, doc_addr) in docs {
                let doc_id = doc_addr.doc_id as usize;
                let score = F32::from(score);
                matched_rows.push((doc_id, Some(score)));
            }
            matched_rows
        } else {
            let collector = DocSetCollector;
            let docs = searcher.search(&query, &collector)?;

            let mut matched_rows = Vec::with_capacity(docs.len());
            for doc_addr in docs {
                let doc_id = doc_addr.doc_id as usize;
                matched_rows.push((doc_id, None));
            }
            matched_rows
        };
        if !matched_rows.is_empty() {
            Ok(Some(matched_rows))
        } else {
            Ok(None)
        }
    }

    // Follow the process below to perform the query search:
    // 1. Read the `fst` first, check if the term in the query matches,
    //    return if it doesn't matched.
    // 2. Read the `term dict` to get the `postings_range` in `idx`
    //    and the `positions_range` in `pos` for each terms.
    // 3. Read the `doc_ids` and `term_freqs` in `idx` for each terms
    //    using `postings_range`.
    // 4. If it's a phrase query, read the `position` of each terms in
    //    `pos` using `positions_range`.
    // 5. Collect matched doc ids using term-related information.
    //
    // If the term does not match, only the `fst` file needs to be read.
    // If the term matches, only the `idx` and `pos` data of the related terms
    // need to be read instead of all the `idx` and `pos` data.
    #[allow(clippy::too_many_arguments)]
    async fn search<'a>(
        &self,
        index_path: &'a str,
        query: Box<dyn Query>,
        field_ids: &HashSet<usize>,
        field_nums: usize,
        need_position: bool,
        has_score: bool,
        index_record: &IndexRecordOption,
        fuzziness: &Option<u8>,
        tokenizer_manager: TokenizerManager,
        row_count: u32,
    ) -> Result<Option<Vec<(usize, Option<F32>)>>> {
        // 1. read index meta
        let inverted_index_meta = load_inverted_index_meta(self.dal.clone(), index_path).await?;

        let inverted_index_meta_map = inverted_index_meta
            .columns
            .clone()
            .into_iter()
            .collect::<HashMap<_, _>>();

        // if meta contains `meta.json` columns,
        // the index file is the first version implementation
        // use compatible search function to read.
        if inverted_index_meta_map.contains_key("meta.json") {
            return self
                .search_v0(
                    index_path,
                    query,
                    field_nums,
                    has_score,
                    tokenizer_manager,
                    row_count,
                )
                .await;
        }

        // 2. read fst files
        let fst_files = self
            .read_column_data(index_path, "fst", field_ids, &inverted_index_meta_map)
            .await?;

        let mut fst_maps = HashMap::new();
        for (field_id, fst_data) in fst_files.into_iter() {
            let fst = Fst::new(fst_data).map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Fst data is corrupted: {:?}", err),
                )
            })?;
            let fst_map = tantivy_fst::Map::from(fst);
            fst_maps.insert(field_id, fst_map);
        }

        // 3. check whether query is matched in the fsts.
        let mut matched_terms = HashMap::new();
        let mut fuzziness_terms = HashMap::new();
        let matched = check_term_fsts_match(
            query.box_clone(),
            &fst_maps,
            fuzziness,
            &mut matched_terms,
            &mut fuzziness_terms,
        );

        if !matched {
            return Ok(None);
        }

        // 4. read term dict files, and get term info for each terms.
        let term_dict_files = self
            .read_column_data(index_path, "term", field_ids, &inverted_index_meta_map)
            .await?;

        let mut term_dict_maps = HashMap::new();
        for (field_id, term_dict_data) in term_dict_files.into_iter() {
            let term_dict_file = FileSlice::new(Arc::new(term_dict_data));
            let term_info_store = TermInfoStore::open(term_dict_file)?;
            term_dict_maps.insert(field_id, term_info_store);
        }

        let mut term_values = HashMap::new();
        for (term, term_ord) in matched_terms.iter() {
            let field = term.field();
            let field_id = field.field_id() as usize;

            let term_dict = term_dict_maps.get(&field_id).unwrap();
            let term_info = term_dict.get(*term_ord);

            let term_value = TermValue {
                term_info,
                doc_ids: vec![],
                term_freqs: vec![],
                position_reader: None,
            };
            term_values.insert(term.clone(), term_value);
        }

        // 5. read postings and optional positions.
        //    collect doc ids, term frequencies and optional position readers.
        let mut slice_metas = Vec::with_capacity(term_values.len());
        let mut name_map = HashMap::new();
        for (term, term_value) in term_values.iter() {
            let field = term.field();
            let field_id = field.field_id() as usize;

            let idx_name = format!("idx-{}", field_id);
            let idx_meta = inverted_index_meta_map.get(&idx_name).unwrap();

            // ignore 8 bytes total_num_tokens_slice
            let offset = idx_meta.offset + 8 + (term_value.term_info.postings_range.start as u64);
            let len = term_value.term_info.postings_range.len() as u64;
            let idx_slice_meta = SingleColumnMeta {
                offset,
                len,
                num_values: 1,
            };

            let idx_slice_name =
                format!("{}-{}", idx_name, term_value.term_info.postings_range.start);
            slice_metas.push((idx_slice_name.clone(), idx_slice_meta));
            name_map.insert(idx_slice_name, term.clone());

            if need_position {
                let pos_name = format!("pos-{}", field_id);
                let pos_meta = inverted_index_meta_map.get(&pos_name).unwrap();
                let offset = pos_meta.offset + (term_value.term_info.positions_range.start as u64);
                let len = term_value.term_info.positions_range.len() as u64;
                let pos_slice_meta = SingleColumnMeta {
                    offset,
                    len,
                    num_values: 1,
                };
                let pos_slice_name = format!(
                    "{}-{}",
                    pos_name, term_value.term_info.positions_range.start
                );
                slice_metas.push((pos_slice_name.clone(), pos_slice_meta));
                name_map.insert(pos_slice_name, term.clone());
            }
        }

        let futs = slice_metas
            .iter()
            .map(|(name, col_meta)| load_inverted_index_file(name, col_meta, index_path, &self.dal))
            .collect::<Vec<_>>();

        let slice_files = try_join_all(futs)
            .await?
            .into_iter()
            .map(|f| (f.name.clone(), f.data.clone()))
            .collect::<HashMap<_, _>>();

        for (slice_name, slice_data) in slice_files.into_iter() {
            let term = name_map.get(&slice_name).unwrap();
            let term_value = term_values.get_mut(term).unwrap();

            if slice_name.starts_with("idx") {
                let posting_file = FileSlice::new(Arc::new(slice_data));
                let postings = BlockSegmentPostings::open(
                    term_value.term_info.doc_freq,
                    posting_file,
                    *index_record,
                    *index_record,
                )?;
                let doc_ids = postings.docs();
                let term_freqs = postings.freqs();

                term_value.doc_ids = doc_ids.to_vec();
                term_value.term_freqs = term_freqs.to_vec();
            } else if slice_name.starts_with("pos") {
                let position_reader = PositionReader::open(slice_data)?;
                term_value.position_reader = Some(position_reader);
            }
        }

        // 6. collect matched rows by term values.
        let matched_docs = collect_matched_rows(
            query.box_clone(),
            row_count,
            &fuzziness_terms,
            &mut term_values,
        );

        if !matched_docs.is_empty() {
            let mut matched_rows = Vec::with_capacity(matched_docs.len());
            if has_score {
                // TODO: add score
                for doc_id in matched_docs {
                    matched_rows.push((doc_id as usize, Some(F32::from(1.0))));
                }
            } else {
                for doc_id in matched_docs {
                    matched_rows.push((doc_id as usize, None))
                }
            }
            Ok(Some(matched_rows))
        } else {
            Ok(None)
        }
    }

    // delegation of [InvertedIndexFileReader::cache_key_of_index_columns]
    pub fn cache_key_of_index_columns(index_path: &str) -> Vec<String> {
        InvertedIndexFileReader::cache_key_of_index_columns(index_path)
    }
}
