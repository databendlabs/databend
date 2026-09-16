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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_search_milliseconds;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use opendal::Operator;
use tantivy::Index;
use tantivy::collector::DocSetCollector;
use tantivy::collector::TopDocs;
use tantivy::query::Query;
use tantivy::tokenizer::TokenizerManager;

use crate::io::read::inverted_index::inverted_index_bundle_loader::load_bundle_search_directory;
use crate::io::read::inverted_index::inverted_index_warmup::InvertedIndexWarmupInfo;

pub type InvertedIndexFilterResult = Option<(Vec<usize>, Option<Vec<F32>>)>;

#[derive(Clone)]
pub struct InvertedIndexReader {
    dal: Operator,
    has_score: bool,
    tokenizer_manager: TokenizerManager,
    warmup: InvertedIndexWarmupInfo,
}

impl InvertedIndexReader {
    pub fn create(
        dal: Operator,
        has_score: bool,
        tokenizer_manager: TokenizerManager,
        warmup: InvertedIndexWarmupInfo,
    ) -> Self {
        Self {
            dal,
            has_score,
            tokenizer_manager,
            warmup,
        }
    }

    // Filter the rows and scores in the block that can match the query text.
    // Only the current raw Tantivy bundle format is supported.
    pub async fn do_filter(
        &self,
        query: Box<dyn Query>,
        index_loc: &str,
        index_format_version: u64,
        index_size: u64,
        row_count: u64,
    ) -> Result<InvertedIndexFilterResult> {
        let start = Instant::now();

        if index_format_version != INVERTED_INDEX_FILE_FORMAT_VERSION {
            return Err(ErrorCode::RefreshIndexError(format!(
                "inverted index `{index_loc}` uses outdated format version {index_format_version}; run `REFRESH TABLE INDEX` to rebuild it"
            )));
        }

        let matched_rows = self
            .bundle_search(index_loc, index_size, query, row_count)
            .await?;
        metrics_inc_block_inverted_index_search_milliseconds(
            u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        );
        Ok(matched_rows)
    }

    async fn bundle_search(
        &self,
        index_path: &str,
        object_size: u64,
        query: Box<dyn Query>,
        row_count: u64,
    ) -> Result<InvertedIndexFilterResult> {
        let bundle = load_bundle_search_directory(&self.dal, index_path, object_size).await?;
        let mut index = Index::open(bundle.directory)?;
        index.set_tokenizers(self.tokenizer_manager.clone());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        if searcher.segment_readers().len() != 1 {
            return Err(ErrorCode::StorageOther(format!(
                "inverted index bundle expects one Tantivy segment, got {}",
                searcher.segment_readers().len()
            )));
        }

        self.warmup.warm_searcher(&searcher, self.has_score).await?;
        self.search(&searcher, query, row_count)
    }

    fn search(
        &self,
        searcher: &tantivy::Searcher,
        query: Box<dyn Query>,
        row_count: u64,
    ) -> Result<InvertedIndexFilterResult> {
        let row_limit = usize::try_from(row_count).map_err(|_| {
            ErrorCode::StorageOther("inverted-index row count exceeds this platform")
        })?;
        let (matched_rows, matched_scores) = if self.has_score {
            let collector = TopDocs::with_limit(row_limit);
            let docs = searcher.search(&query, &collector.order_by_score())?;

            let mut matched_rows = Vec::with_capacity(docs.len());
            let mut matched_scores = Vec::with_capacity(docs.len());
            for (score, doc_addr) in docs {
                matched_rows.push(usize::try_from(doc_addr.doc_id).map_err(|_| {
                    ErrorCode::StorageOther("inverted-index document id exceeds this platform")
                })?);
                matched_scores.push(F32::from(score));
            }
            (matched_rows, Some(matched_scores))
        } else {
            let docs = searcher.search(&query, &DocSetCollector)?;
            let matched_rows = docs
                .into_iter()
                .map(|doc_addr| {
                    usize::try_from(doc_addr.doc_id).map_err(|_| {
                        ErrorCode::StorageOther("inverted-index document id exceeds this platform")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            (matched_rows, None)
        };

        Ok((!matched_rows.is_empty()).then_some((matched_rows, matched_scores)))
    }
}
