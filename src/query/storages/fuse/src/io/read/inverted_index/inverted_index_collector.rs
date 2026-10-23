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

use databend_common_expression::types::F32;
use tantivy::DocId;
use tantivy::Score;
use tantivy::SegmentOrdinal;
use tantivy::SegmentReader;
use tantivy::collector::Collector;
use tantivy::collector::SegmentCollector;

/// Collect every matching row, without hashing DocAddresses or ranking by score.
/// Bundle indexes have one segment, so DocIds correspond directly to block rows.
pub(super) struct InvertedIndexCollector {
    has_score: bool,
}

impl InvertedIndexCollector {
    pub(super) fn new(has_score: bool) -> Self {
        Self { has_score }
    }
}

/// Scores, when requested, remain aligned with rows in collection order.
pub(super) struct MatchedDocs {
    pub(super) rows: Vec<DocId>,
    pub(super) scores: Option<Vec<F32>>,
}

impl MatchedDocs {
    fn new(has_score: bool) -> Self {
        Self {
            rows: Vec::new(),
            scores: has_score.then(Vec::new),
        }
    }
}

impl Collector for InvertedIndexCollector {
    type Fruit = MatchedDocs;
    type Child = MatchedDocs;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        _segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        if segment_ord != 0 {
            return Err(tantivy::TantivyError::InvalidArgument(
                "inverted-index collector expects one segment".to_owned(),
            ));
        }
        Ok(MatchedDocs::new(self.has_score))
    }

    fn requires_scoring(&self) -> bool {
        self.has_score
    }

    fn merge_fruits(&self, mut fruits: Vec<MatchedDocs>) -> tantivy::Result<MatchedDocs> {
        if fruits.len() > 1 {
            return Err(tantivy::TantivyError::InvalidArgument(
                "inverted-index collector expects one segment".to_owned(),
            ));
        }
        Ok(fruits
            .pop()
            .unwrap_or_else(|| MatchedDocs::new(self.has_score)))
    }
}

impl SegmentCollector for MatchedDocs {
    type Fruit = Self;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.rows.push(doc);
        if let Some(scores) = &mut self.scores {
            scores.push(F32::from(score));
        }
    }

    fn collect_block(&mut self, docs: &[DocId]) {
        // Tantivy uses this path for unscored searches; preserve the trait's default
        // zero-score behavior if it is ever called on a scored collector.
        if self.scores.is_some() {
            for &doc in docs {
                self.collect(doc, 0.0);
            }
        } else {
            self.rows.extend_from_slice(docs);
        }
    }

    fn harvest(self) -> Self::Fruit {
        self
    }
}

#[cfg(test)]
mod tests {
    use tantivy::Index;
    use tantivy::collector::DocSetCollector;
    use tantivy::collector::TopDocs;
    use tantivy::query::QueryParser;
    use tantivy::schema::Schema;
    use tantivy::schema::TEXT;

    use super::*;

    #[test]
    fn test_matches_reference_collectors() -> tantivy::Result<()> {
        let mut schema = Schema::builder();
        let body = schema.add_text_field("body", TEXT);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads(1, 50_000_000)?;
        for text in ["alpha beta", "alpha alpha", "gamma", "beta alpha gamma"] {
            writer.add_document(tantivy::doc!(body => text))?;
        }
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        assert!(
            InvertedIndexCollector::new(false)
                .for_segment(1, &searcher.segment_readers()[0])
                .is_err()
        );
        let parser = QueryParser::for_index(&index, vec![body]);
        for text in [
            "alpha",
            "alpha^2",
            "alpha OR beta",
            "\"alpha beta\"",
            "missing",
        ] {
            let query = parser.parse_query(text)?;
            let plain = searcher.search(query.as_ref(), &InvertedIndexCollector::new(false))?;
            assert!(plain.scores.is_none());
            let mut expected_rows = searcher
                .search(query.as_ref(), &DocSetCollector)?
                .into_iter()
                .map(|address| address.doc_id)
                .collect::<Vec<_>>();
            expected_rows.sort_unstable();
            let mut rows = plain.rows;
            rows.sort_unstable();
            assert_eq!(rows, expected_rows, "{text}");

            let scored = searcher.search(query.as_ref(), &InvertedIndexCollector::new(true))?;
            let scores = scored.scores.unwrap();
            assert_eq!(scored.rows.len(), scores.len());
            let mut actual = scored.rows.into_iter().zip(scores).collect::<Vec<_>>();
            actual.sort_unstable_by_key(|(row, _)| *row);
            let mut expected = searcher
                .search(query.as_ref(), &TopDocs::with_limit(4).order_by_score())?
                .into_iter()
                .map(|(score, address)| (address.doc_id, F32::from(score)))
                .collect::<Vec<_>>();
            expected.sort_unstable_by_key(|(row, _)| *row);
            assert_eq!(actual, expected, "{text}");
        }
        Ok(())
    }

    #[test]
    fn test_block_collection_and_single_segment_guard() -> tantivy::Result<()> {
        let collector = InvertedIndexCollector::new(false);
        let mut docs = MatchedDocs::new(false);
        docs.collect_block(&[1, 3, 5]);
        docs.collect(7, 0.0);
        let docs = collector.merge_fruits(vec![docs.harvest()])?;
        assert_eq!(docs.rows, vec![1, 3, 5, 7]);
        assert!(docs.scores.is_none());
        assert!(
            collector
                .merge_fruits(vec![MatchedDocs::new(false), MatchedDocs::new(false)])
                .is_err()
        );
        let empty = InvertedIndexCollector::new(true).merge_fruits(Vec::new())?;
        assert!(empty.rows.is_empty());
        assert_eq!(empty.scores, Some(Vec::new()));
        Ok(())
    }
}
