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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_storages_common_index::InvertedIndexDirectory;
use opendal::Operator;
use tantivy::collector::DocSetCollector;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Field;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::JsonObjectOptions;
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::tokenizer::TokenizerManager;
use tantivy::Index;
use tantivy::Score;

use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_directory;
use crate::io::write::create_tokenizer_manager;

#[derive(Clone)]
pub struct InvertedIndexReader {
    has_score: bool,
    fields: Vec<Field>,
    field_boosts: Vec<(Field, Score)>,
    directory: InvertedIndexDirectory,
    tokenizer_manager: TokenizerManager,
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

        let filters: HashSet<String> = match inverted_index_info.index_options.get("filters") {
            Some(filters_str) => filters_str.split(',').map(|v| v.to_string()).collect(),
            None => HashSet::new(),
        };
        let tokenizer_manager = create_tokenizer_manager(&filters);

        let tokenizer_name = inverted_index_info
            .index_options
            .get("tokenizer")
            .cloned()
            .unwrap_or("english".to_string());

        let index_record: IndexRecordOption =
            match &inverted_index_info.index_options.get("index_record") {
                Some(v) => serde_json::from_str(v)?,
                None => IndexRecordOption::WithFreqsAndPositions,
            };

        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer(&tokenizer_name)
            .set_index_option(index_record);
        let text_options = TextOptions::default().set_indexing_options(text_field_indexing.clone());
        let json_options = JsonObjectOptions::default().set_indexing_options(text_field_indexing);

        let mut schema_builder = Schema::builder();
        let mut default_fields = Vec::with_capacity(inverted_index_info.index_schema.num_fields());
        for (i, field) in inverted_index_info.index_schema.fields().iter().enumerate() {
            match field.data_type().remove_nullable() {
                DataType::String => {
                    schema_builder.add_text_field(field.name(), text_options.clone());
                }
                DataType::Variant => {
                    schema_builder.add_json_field(field.name(), json_options.clone());
                }
                _ => {
                    return Err(ErrorCode::IllegalDataType(format!(
                        "inverted index only support String and Variant type, but got {}",
                        field.data_type()
                    )));
                }
            }
            default_fields.push(Field::from_field_id(i as u32));
        }
        let schema = schema_builder.build();
        let query_parser = QueryParser::new(schema, default_fields, tokenizer_manager.clone());
        let query = query_parser.parse_query(&inverted_index_info.query_text)?;

        let mut need_position = false;
        query.query_terms(&mut |term, pos| {
            println!("term={:?} pos={:?}", term, pos);
            if pos {
                need_position = true;
            }
        });

        let field_nums = inverted_index_info.index_schema.num_fields();
        let directory =
            load_inverted_index_directory(dal.clone(), need_position, field_nums, index_loc)
                .await?;

        let has_score = inverted_index_info.has_score;

        Ok(Self {
            has_score,
            fields,
            field_boosts,
            directory,
            tokenizer_manager,
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
        let mut index = Index::open(self.directory)?;
        index.set_tokenizers(self.tokenizer_manager);
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
