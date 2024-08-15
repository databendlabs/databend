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

use std::time::Instant;

use databend_common_catalog::plan::InvertedIndexOption;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_search_milliseconds;
use databend_storages_common_index::InvertedIndexDirectory;
use opendal::Operator;
use tantivy::collector::DocSetCollector;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Field;
use tantivy::tokenizer::TokenizerManager;
use tantivy::Index;
use tantivy::Score;

use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_directory;
use crate::io::read::inverted_index::inverted_index_loader::InvertedIndexFileReader;

#[derive(Clone)]
pub struct InvertedIndexReader {
    directory: InvertedIndexDirectory,
}

impl InvertedIndexReader {
    pub async fn try_create(
        dal: Operator,
        field_nums: usize,
        need_position: bool,
        index_loc: &str,
    ) -> Result<Self> {
        let directory =
            load_inverted_index_directory(dal.clone(), need_position, field_nums, index_loc)
                .await?;

        Ok(Self { directory })
    }

    // Filter the rows and scores in the block that can match the query text,
    // if there is no row that can match, this block can be pruned.
    #[allow(clippy::type_complexity)]
    pub fn do_filter(
        self,
        query: &str,
        has_score: bool,
        query_fields: &Vec<Field>,
        query_field_boosts: &Vec<(Field, Score)>,
        tokenizer_manager: TokenizerManager,
        inverted_index_option: &Option<InvertedIndexOption>,
        row_count: u64,
    ) -> Result<Option<Vec<(usize, Option<F32>)>>> {
        let start = Instant::now();
        let mut index = Index::open(self.directory)?;
        index.set_tokenizers(tokenizer_manager);
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let mut query_parser = QueryParser::for_index(&index, query_fields.clone());
        // set optional boost value for the field
        for (field, boost) in query_field_boosts {
            query_parser.set_field_boost(*field, *boost);
        }
        let fuzziness = inverted_index_option
            .as_ref()
            .and_then(|o| o.fuzziness.as_ref());
        if let Some(fuzziness) = fuzziness {
            for field in query_fields {
                query_parser.set_field_fuzzy(*field, true, *fuzziness, true);
            }
        }
        let operator = inverted_index_option
            .as_ref()
            .map(|o| o.operator)
            .unwrap_or_default();
        if operator {
            query_parser.set_conjunction_by_default();
        }
        let lenient = inverted_index_option
            .as_ref()
            .map(|o| o.lenient)
            .unwrap_or_default();
        let query = if lenient {
            let (query, _) = query_parser.parse_query_lenient(query);
            query
        } else {
            query_parser.parse_query(query)?
        };

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

        // Perf.
        {
            metrics_inc_block_inverted_index_search_milliseconds(start.elapsed().as_millis() as u64);
        }

        if !matched_rows.is_empty() {
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
