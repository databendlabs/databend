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

use std::collections::HashSet;

use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_storages_common_index::InvertedIndexDirectory;
use opendal::Operator;
use tantivy::collector::DocSetCollector;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Field;
use tantivy::Index;
use tantivy::Score;

use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_directory;
use crate::io::write::create_tokenizer_manager;

#[derive(Clone)]
pub struct InvertedIndexReader {
    has_score: bool,
    fields: Vec<Field>,
    field_boosts: Vec<(Field, Score)>,
    filters: HashSet<String>,
    directory: InvertedIndexDirectory,
}

impl InvertedIndexReader {
    pub async fn try_create(
        dal: Operator,
        inverted_index_info: &InvertedIndexInfo,
        index_loc: &str,
    ) -> Result<Self> {
        let mut fields = Vec::with_capacity(inverted_index_info.query_fields.len());
        let mut field_boosts = Vec::with_capacity(inverted_index_info.query_fields.len());
        for (field_name, boost) in &inverted_index_info.query_fields {
            let i = inverted_index_info.index_schema.index_of(field_name)?;
            let field = Field::from_field_id(i as u32);
            fields.push(field);
            if let Some(boost) = boost {
                field_boosts.push((field, boost.0));
            }
        }
        // read tantivy position file only when query has phrase terms
        let need_position = inverted_index_info.query_text.contains('"');

        let field_nums = inverted_index_info.index_schema.num_fields();
        let directory =
            load_inverted_index_directory(dal.clone(), need_position, field_nums, index_loc)
                .await?;

        let filters: HashSet<String> = match inverted_index_info.index_options.get("filters") {
            Some(filters_str) => filters_str.split(',').map(|v| v.to_string()).collect(),
            None => HashSet::new(),
        };
        let has_score = inverted_index_info.has_score;

        Ok(Self {
            has_score,
            fields,
            field_boosts,
            filters,
            directory,
        })
    }

    // Filter the rows and scores in the block that can match the query text,
    // if there is no row that can match, this block can be pruned.
    #[allow(clippy::type_complexity)]
    pub fn do_filter(
        self,
        query: &str,
        row_count: u64,
    ) -> Result<Option<Vec<(usize, Option<F32>)>>> {
        let tokenizer_manager = create_tokenizer_manager(&self.filters);
        let mut index = Index::open(self.directory)?;
        index.set_tokenizers(tokenizer_manager.clone());
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let mut query_parser = QueryParser::for_index(&index, self.fields.clone());
        // set optional boost value for the field
        for (field, boost) in &self.field_boosts {
            query_parser.set_field_boost(*field, *boost);
        }
        let query = query_parser.parse_query(query)?;

        let mut matched_rows = Vec::new();
        if self.has_score {
            let collector = TopDocs::with_limit(row_count as usize);
            let docs = searcher.search(&query, &collector)?;

            if docs.is_empty() {
                return Ok(None);
            }
            for (score, doc_addr) in docs {
                let doc_id = doc_addr.doc_id as usize;
                let score = F32::from(score);
                matched_rows.push((doc_id, Some(score)));
            }
        } else {
            let collector = DocSetCollector;
            let docs = searcher.search(&query, &collector)?;

            if docs.is_empty() {
                return Ok(None);
            }
            for doc_addr in docs {
                let doc_id = doc_addr.doc_id as usize;
                matched_rows.push((doc_id, None));
            }
        }

        Ok(Some(matched_rows))
    }
}
