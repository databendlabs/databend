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

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_expression::DataSchema;
use databend_storages_common_index::InvertedIndexDirectory;
use databend_storages_common_table_meta::meta::IndexSegmentInfo;
use futures_util::future::try_join_all;
use opendal::Operator;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Field;
use tantivy::tokenizer::Language;
use tantivy::tokenizer::LowerCaser;
use tantivy::tokenizer::RemoveLongFilter;
use tantivy::tokenizer::SimpleTokenizer;
use tantivy::tokenizer::Stemmer;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::tokenizer::TokenizerManager;
use tantivy::Index;

use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_filter;

#[derive(Clone)]
pub struct InvertedIndexReader {
    fields: Vec<Field>,
    directories: Vec<Arc<InvertedIndexDirectory>>,
}

impl InvertedIndexReader {
    pub async fn try_create(
        dal: Operator,
        schema: &DataSchema,
        query_columns: &Vec<String>,
        indexes: &BTreeMap<String, Vec<IndexSegmentInfo>>,
    ) -> Result<Self> {
        let mut fields = Vec::with_capacity(query_columns.len());
        for column_name in query_columns {
            let i = schema.index_of(column_name)?;
            let field = Field::from_field_id(i as u32);
            fields.push(field);
        }

        let futs = indexes
            .iter()
            .map(|(index_loc, index_segments)| {
                load_inverted_index_filter(dal.clone(), index_loc.clone(), index_segments.clone())
            })
            .collect::<Vec<_>>();

        let directories = try_join_all(futs).await?.into_iter().collect();

        Ok(Self {
            fields,
            directories,
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn do_filter(&self, query: &str) -> Result<BTreeMap<String, Option<Vec<(usize, F32)>>>> {
        let tokenizer_manager = TokenizerManager::new();
        tokenizer_manager.register(
            "en",
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .filter(Stemmer::new(Language::English))
                .build(),
        );

        let mut segment_map = BTreeMap::new();
        for directory in &self.directories {
            let index_segments = directory.index_segments().clone();
            let num = index_segments.iter().map(|i| i.row_count).sum::<u64>() as usize;

            let directory = Arc::unwrap_or_clone(directory.clone());
            let mut index = Index::open(directory)?;
            index.set_tokenizers(tokenizer_manager.clone());
            let reader = index.reader()?;
            let searcher = reader.searcher();

            let query_parser = QueryParser::for_index(&index, self.fields.clone());
            let query = query_parser.parse_query(query)?;

            // TODO: support TopN
            let collector = TopDocs::with_limit(num);
            let docs = searcher.search(&query, &collector)?;

            let mut doc_id_scores = VecDeque::new();
            for (score, doc_addr) in docs {
                let doc_id = doc_addr.doc_id;
                doc_id_scores.push_back((doc_id, score));
            }

            // Converts the doc id in the index to the row id in each segment
            // by the row count in each segments.
            let mut row_count: u32 = 0;
            let mut next_row_count: u32 = 0;
            let mut row_id_scores = Vec::new();
            for index_segment in index_segments {
                next_row_count += index_segment.row_count as u32;
                while let Some((doc_id, score)) = doc_id_scores.front() {
                    if *doc_id < next_row_count {
                        let row_id = doc_id - row_count;
                        row_id_scores.push((row_id as usize, F32::from(*score)));
                        doc_id_scores.pop_front();
                    } else {
                        break;
                    }
                }
                let segment_row_id_scores = if !row_id_scores.is_empty() {
                    let segment_row_id_scores = std::mem::take(&mut row_id_scores);
                    Some(segment_row_id_scores)
                } else {
                    None
                };
                segment_map.insert(
                    index_segment.segment_location.clone(),
                    segment_row_id_scores,
                );
                row_count = next_row_count;
            }
        }

        Ok(segment_map)
    }
}
