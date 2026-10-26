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
    top_k: Option<usize>,
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
            top_k: None,
            tokenizer_manager,
            warmup,
        }
    }

    /// Set a per-block score limit after checking `PushDownInfo::inverted_index_top_k`.
    /// Unscored searches always collect the complete match set.
    pub fn with_top_k(mut self, top_k: Option<usize>) -> Self {
        self.top_k = top_k;
        self
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
        if index_format_version != INVERTED_INDEX_FILE_FORMAT_VERSION {
            return Err(ErrorCode::RefreshIndexError(format!(
                "inverted index `{index_loc}` uses outdated format version {index_format_version}; run `REFRESH INVERTED INDEX` to rebuild it"
            )));
        }

        self.bundle_search(index_loc, index_size, query, row_count)
            .await
    }

    async fn bundle_search(
        &self,
        index_path: &str,
        object_size: u64,
        query: Box<dyn Query>,
        row_count: u64,
    ) -> Result<InvertedIndexFilterResult> {
        let bundle = load_bundle_search_directory(&self.dal, index_path, object_size).await?;
        let mut index = Index::open(bundle.directory.clone())?;
        index.set_tokenizers(self.tokenizer_manager.clone());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        if searcher.segment_readers().len() != 1 {
            return Err(ErrorCode::StorageOther(format!(
                "inverted index bundle expects one Tantivy segment, got {}",
                searcher.segment_readers().len()
            )));
        }

        self.warmup
            .warm_searcher(&searcher, &bundle.directory, self.has_score)
            .await?;
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
        // TopDocs rejects a zero limit. Empty blocks and LIMIT 0 have no candidates.
        let score_limit = self.top_k.unwrap_or(row_limit).min(row_limit);
        if row_limit == 0 || (self.has_score && score_limit == 0) {
            return Ok(None);
        }
        let start = Instant::now();
        let (matched_rows, matched_scores) = if self.has_score {
            let collector = TopDocs::with_limit(score_limit);
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
        metrics_inc_block_inverted_index_search_milliseconds(
            u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        );

        Ok((!matched_rows.is_empty()).then_some((matched_rows, matched_scores)))
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use opendal::services::Memory;
    use tantivy::query::QueryParser;
    use tantivy::schema::Field;
    use tantivy::schema::Schema;
    use tantivy::schema::TEXT;

    use super::*;

    // Span multiple Tantivy posting blocks to exercise score-based skipping too.
    const ROWS: usize = 512;

    fn index(block: usize) -> (Index, Field) {
        let mut schema = Schema::builder();
        let body = schema.add_text_field("body", TEXT);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
        for row in 0..ROWS {
            let text = if row % 5 == 0 {
                "gamma delta".to_string()
            } else {
                format!(
                    "{} beta {}",
                    "alpha ".repeat(1 + (row + block) % 17),
                    "delta ".repeat(1 + row % 7)
                )
            };
            writer.add_document(tantivy::doc!(body => text)).unwrap();
        }
        writer.commit().unwrap();
        (index, body)
    }

    fn reader(has_score: bool, top_k: Option<usize>) -> InvertedIndexReader {
        InvertedIndexReader::create(
            Operator::new(Memory::default()).unwrap().finish(),
            has_score,
            TokenizerManager::default(),
            InvertedIndexWarmupInfo::default(),
        )
        .with_top_k(top_k)
    }

    fn search(
        index: &Index,
        body: Field,
        text: &str,
        has_score: bool,
        top_k: Option<usize>,
        row_count: usize,
    ) -> InvertedIndexFilterResult {
        let parser = QueryParser::for_index(index, vec![body]);
        reader(has_score, top_k)
            .search(
                &index.reader().unwrap().searcher(),
                parser.parse_query(text).unwrap(),
                row_count as u64,
            )
            .unwrap()
    }

    #[test]
    fn test_score_top_k_matches_full_collection() {
        let (index, body) = index(0);
        // Include ties, phrase/Boolean queries, and an empty match set. Compare
        // both document ids and the unmodified BM25 scores with full collection.
        for text in [
            "alpha",
            "\"alpha beta\"",
            "alpha OR gamma",
            "alpha^2 OR gamma",
            "alpha OR beta OR gamma",
            "alpha AND beta",
            "absent",
        ] {
            let (all_rows, all_scores) =
                search(&index, body, text, true, None, ROWS).unwrap_or_default();
            let all_scores = all_scores.unwrap_or_default();
            for k in [0, 1, 10, ROWS - 1, ROWS, ROWS + 1, usize::MAX] {
                let (rows, scores) =
                    search(&index, body, text, true, Some(k), ROWS).unwrap_or_default();
                let n = k.min(all_rows.len());
                assert_eq!(rows, all_rows[..n], "query={text}, k={k}");
                assert_eq!(
                    scores.unwrap_or_default(),
                    all_scores[..n],
                    "query={text}, k={k}"
                );
            }
        }
        assert!(search(&index, body, "alpha", true, Some(10), 0).is_none());
    }

    #[test]
    fn test_unscored_search_does_not_truncate_matches() {
        let (index, body) = index(0);
        let (mut expected, scores) = search(&index, body, "alpha", false, None, ROWS).unwrap();
        expected.sort_unstable();
        assert!(scores.is_none());
        assert!(expected.len() > 10);
        for k in [0, 1, 10] {
            let (mut rows, scores) = search(&index, body, "alpha", false, Some(k), ROWS).unwrap();
            rows.sort_unstable();
            assert_eq!(rows, expected);
            assert!(scores.is_none());
        }
        assert!(search(&index, body, "alpha", false, Some(10), 0).is_none());
    }

    #[test]
    fn test_per_block_top_k_preserves_global_top_k_and_offset() {
        let indices = (0..3).map(index).collect::<Vec<_>>();
        let collect = |top_k| {
            let mut docs = Vec::new();
            for (block, (index, body)) in indices.iter().enumerate() {
                let (rows, scores) =
                    search(index, *body, "alpha OR gamma", true, top_k, ROWS).unwrap();
                docs.extend(
                    rows.into_iter()
                        .zip(scores.unwrap())
                        .map(|(row, score)| (score, block, row)),
                );
            }
            // Use the same deterministic tie-break in both reference and TopK.
            docs.sort_by_key(|(score, block, row)| (Reverse(*score), *block, *row));
            docs
        };
        let all = collect(None);
        for offset in [0, 1, 15, ROWS * 3] {
            for limit in [1, 10, 50] {
                let candidates = collect(Some(offset + limit));
                assert!(candidates.len() <= indices.len() * (offset + limit).min(ROWS));
                assert_eq!(
                    candidates
                        .into_iter()
                        .skip(offset)
                        .take(limit)
                        .collect::<Vec<_>>(),
                    all.iter()
                        .copied()
                        .skip(offset)
                        .take(limit)
                        .collect::<Vec<_>>(),
                    "offset={offset}, limit={limit}"
                );
            }
        }
    }
}
