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

use databend_common_exception::Result;
use databend_common_expression::DataSchema;
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
use tantivy::DocAddress;
use tantivy::Index;
use tantivy::Score;

use crate::io::read::inverted_index::cache_directory::CacheDirectory;

#[derive(Clone)]
pub struct InvertedIndexReader {
    dal: Operator,
    schema: DataSchema,
}

impl InvertedIndexReader {
    pub fn create(dal: Operator, schema: DataSchema) -> Self {
        Self { dal, schema }
    }

    pub fn do_read(
        &self,
        path: String,
        query: &str,
        num: usize,
    ) -> Result<Vec<(Score, DocAddress)>> {
        let data = self.dal.blocking().read_with(&path).call()?;

        let directory = CacheDirectory::try_create(data)?;
        let mut index = Index::open(directory)?;
        let tokenizer_manager = TokenizerManager::new();
        tokenizer_manager.register(
            "en",
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .filter(Stemmer::new(Language::English))
                .build(),
        );
        index.set_tokenizers(tokenizer_manager);

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let fields = (0..self.schema.fields.len())
            .map(|i| Field::from_field_id(i as u32))
            .collect::<Vec<_>>();

        let query_parser = QueryParser::for_index(&index, fields);

        let query = query_parser.parse_query(query)?;
        let collector = TopDocs::with_limit(num);
        let docs = searcher.search(&query, &collector)?;

        Ok(docs)
    }
}
