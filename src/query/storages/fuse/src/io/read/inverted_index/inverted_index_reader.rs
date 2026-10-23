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

use databend_common_base::runtime::spawn_blocking;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_metrics::storage::InvertedIndexPhase;
use databend_common_metrics::storage::InvertedIndexTimer;
use databend_common_metrics::storage::metrics_observe_block_inverted_index_warmup_bytes;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use opendal::Operator;
use tantivy::Index;
use tantivy::query::Query;
use tantivy::tokenizer::TokenizerManager;

use crate::io::read::inverted_index::inverted_index_bundle_loader::load_bundle_search_directory;
use crate::io::read::inverted_index::inverted_index_collector::InvertedIndexCollector;
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
        let open_timer = InvertedIndexTimer::new(InvertedIndexPhase::Open);
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

        drop(open_timer);

        let can_match = {
            let _timer = InvertedIndexTimer::new(InvertedIndexPhase::Warmup);
            self.warmup
                .warm_searcher(&searcher, &bundle.directory, self.has_score)
                .await?
        };
        metrics_observe_block_inverted_index_warmup_bytes(bundle.search_pin.pinned_bytes());
        if !can_match {
            return Ok(None);
        }
        Self::search(searcher, query, row_count, self.has_score).await
    }

    async fn search(
        searcher: tantivy::Searcher,
        query: Box<dyn Query>,
        row_count: u64,
        has_score: bool,
    ) -> Result<InvertedIndexFilterResult> {
        // Searcher owns the index/directory and keeps the warmed ranges pinned until the
        // blocking task finishes, even if the caller drops its IndexReader or is cancelled.
        // Production callers await this task while holding their block-pruning permit.
        let queue_timer = InvertedIndexTimer::new(InvertedIndexPhase::SearchQueue);
        spawn_blocking(move || {
            drop(queue_timer);
            // Execution wall time includes collection/conversion, but excludes queueing.
            let _timer = InvertedIndexTimer::new(InvertedIndexPhase::Search);
            let row_limit = usize::try_from(row_count).map_err(|_| {
                ErrorCode::StorageOther("inverted-index row count exceeds this platform")
            })?;
            let docs = searcher.search(query.as_ref(), &InvertedIndexCollector::new(has_score))?;
            debug_assert!(docs.rows.len() <= row_limit);
            let matched_rows = docs
                .rows
                .into_iter()
                .map(|doc| {
                    usize::try_from(doc).map_err(|_| {
                        ErrorCode::StorageOther("inverted-index document id exceeds this platform")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let matched_scores = docs.scores;
            Ok((!matched_rows.is_empty()).then_some((matched_rows, matched_scores)))
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use std::thread::ThreadId;

    use databend_common_base::runtime::ThreadTracker;
    use databend_common_base::runtime::metrics::MetricValue;
    use databend_common_base::runtime::metrics::ScopedRegistry;
    use tantivy::Term;
    use tantivy::query::EnableScoring;
    use tantivy::query::TermQuery;
    use tantivy::query::Weight;
    use tantivy::schema::IndexRecordOption;
    use tantivy::schema::Schema;
    use tantivy::schema::TEXT;

    use super::*;

    #[derive(Clone, Debug)]
    struct ThreadCheckedQuery {
        inner: Box<dyn Query>,
        async_thread: ThreadId,
        fail: bool,
    }

    impl Query for ThreadCheckedQuery {
        fn weight(&self, scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
            // On a current-thread runtime, this fails if search moves back onto the async worker.
            assert_ne!(std::thread::current().id(), self.async_thread);
            if self.fail {
                return Err(tantivy::TantivyError::InvalidArgument(
                    "injected search failure".to_owned(),
                ));
            }
            self.inner.weight(scoring)
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_blocking_search_preserves_results_and_errors() -> Result<()> {
        let mut schema = Schema::builder();
        let body = schema.add_text_field("body", TEXT);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads(1, 50_000_000)?;
        writer.add_document(tantivy::doc!(body => "alpha"))?;
        writer.add_document(tantivy::doc!(body => "beta"))?;
        writer.commit()?;
        drop(writer);
        let reader = index.reader()?;
        let searcher = reader.searcher();
        // The owned searcher must keep the index/directory alive on its own.
        drop(reader);
        drop(index);

        let metrics = ScopedRegistry::create(None);
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.metrics = Some(metrics.clone());
        let _tracking = ThreadTracker::tracking(payload);

        for (word, has_score, fail) in [
            ("alpha", false, false),
            ("alpha", true, false),
            ("missing", false, false),
            ("missing", true, false),
            ("alpha", false, true),
        ] {
            let query = ThreadCheckedQuery {
                inner: Box::new(TermQuery::new(
                    Term::from_field_text(body, word),
                    IndexRecordOption::WithFreqs,
                )),
                async_thread: std::thread::current().id(),
                fail,
            };
            let result =
                InvertedIndexReader::search(searcher.clone(), Box::new(query), 2, has_score).await;
            if fail {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("injected search failure")
                );
            } else if word == "missing" {
                assert!(result?.is_none());
            } else {
                let (rows, scores) = result?.unwrap();
                assert_eq!(rows, vec![0]);
                if has_score {
                    let scores = scores.unwrap();
                    assert_eq!(scores.len(), rows.len());
                    assert!(scores[0].is_finite() && scores[0].0 > 0.0);
                } else {
                    assert!(scores.is_none());
                }
            }
        }
        let samples = metrics.dump_sample()?;
        for name in [
            "fuse_block_inverted_index_search_queue_milliseconds",
            "fuse_block_inverted_index_search_milliseconds",
        ] {
            let sample = samples.iter().find(|sample| sample.name == name).unwrap();
            let MetricValue::Histogram(buckets) = &sample.value else {
                panic!("expected a histogram for {name}");
            };
            // Four successful searches and one failed search, all tracked in this query scope.
            assert_eq!(buckets.last().unwrap().count, 5.0, "{name}");
        }
        Ok(())
    }
}
