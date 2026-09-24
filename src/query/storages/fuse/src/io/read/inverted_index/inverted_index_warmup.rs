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

use databend_common_exception::Result;
use futures_util::future;
use tantivy::Searcher;
use tantivy::Term;
use tantivy::index::SegmentComponent;
use tantivy::query::Query;
use tantivy::query_grammar::UserInputAst;
use tantivy::query_grammar::UserInputLeaf;
use tantivy::schema::Field;
use tantivy::schema::Schema;

/// The ranges to pin before Tantivy's synchronous search. The executable query supplies exact
/// terms; the input AST only identifies operations whose terms `query_terms()` cannot enumerate.
#[derive(Clone, Default)]
pub struct InvertedIndexWarmupInfo {
    exact_postings: HashMap<Term, bool>,
    full_posting_fields: HashMap<Field, bool>,
    dictionary_fields: HashSet<Field>,
    fast_fields: HashSet<Field>,
}

impl InvertedIndexWarmupInfo {
    pub fn try_create(
        query: &dyn Query,
        ast: &UserInputAst,
        schema: &Schema,
        fallback_fields: &[Field],
        fuzziness: Option<u8>,
    ) -> Result<Self> {
        let mut plan = Self::default();
        plan.collect_exact_terms(query);
        plan.collect_ast_requirements(ast, schema, fallback_fields, fuzziness);
        plan.normalize();
        Ok(plan)
    }

    fn collect_exact_terms(&mut self, query: &dyn Query) {
        query.query_terms(&mut |term, with_positions| {
            *self.exact_postings.entry(term.clone()).or_default() |= with_positions;
        });
    }

    fn collect_ast_requirements(
        &mut self,
        ast: &UserInputAst,
        schema: &Schema,
        fallback_fields: &[Field],
        fuzziness: Option<u8>,
    ) {
        match ast {
            UserInputAst::Clause(clauses) => {
                for (_, child) in clauses {
                    self.collect_ast_requirements(child, schema, fallback_fields, fuzziness);
                }
            }
            UserInputAst::Boost(child, _) => {
                self.collect_ast_requirements(child, schema, fallback_fields, fuzziness);
            }
            UserInputAst::Leaf(leaf) => match leaf.as_ref() {
                UserInputLeaf::Literal(literal) if literal.prefix || fuzziness.is_some() => {
                    // A phrase prefix requires positions for both fixed and expanded terms.
                    self.for_fields(
                        schema,
                        literal.field_name.as_deref(),
                        fallback_fields,
                        |plan, field| {
                            if schema.get_field_entry(field).is_indexed() {
                                plan.add_full(field, literal.prefix);
                            }
                        },
                    );
                }
                UserInputLeaf::Regex { field, .. } => {
                    self.for_fields(schema, field.as_deref(), fallback_fields, |plan, field| {
                        if schema.get_field_entry(field).is_indexed() {
                            plan.add_full(field, false);
                        }
                    });
                }
                UserInputLeaf::Range { field, .. } => {
                    self.for_fields(schema, field.as_deref(), fallback_fields, |plan, field| {
                        let entry = schema.get_field_entry(field);
                        if entry.is_fast() {
                            plan.fast_fields.insert(field);
                        } else if entry.is_indexed() {
                            plan.add_full(field, false);
                        }
                    });
                }
                UserInputLeaf::Set { field, .. } => {
                    // TermSetQuery reports its terms but also scans the dictionary with an FST.
                    self.for_fields(schema, field.as_deref(), fallback_fields, |plan, field| {
                        if schema.get_field_entry(field).is_indexed() {
                            plan.dictionary_fields.insert(field);
                        }
                    });
                }
                UserInputLeaf::Exists { field } => {
                    // Depending on the field, ExistsQuery may need indexed terms or fast fields.
                    self.for_fields(schema, Some(field), fallback_fields, |plan, field| {
                        let entry = schema.get_field_entry(field);
                        if entry.is_indexed() {
                            plan.add_full(field, false);
                        }
                        if entry.is_fast() {
                            plan.fast_fields.insert(field);
                        }
                    });
                }
                UserInputLeaf::Literal(_) | UserInputLeaf::All => {}
            },
        }
    }

    /// Resolve an explicit field (including JSON paths); an unknown name may resolve to a JSON
    /// path on a default field, so fall back to every indexed/fast field rather than guessing.
    fn for_fields(
        &mut self,
        schema: &Schema,
        name: Option<&str>,
        fallback_fields: &[Field],
        mut visit: impl FnMut(&mut Self, Field),
    ) {
        if let Some(name) = name {
            if let Some((field, _)) = schema.find_field(name) {
                visit(self, field);
                return;
            }
            for (field, entry) in schema.fields() {
                if entry.is_indexed() || entry.is_fast() {
                    visit(self, field);
                }
            }
        } else {
            for &field in fallback_fields {
                visit(self, field);
            }
        }
    }

    fn add_full(&mut self, field: Field, with_positions: bool) {
        *self.full_posting_fields.entry(field).or_default() |= with_positions;
    }

    fn normalize(&mut self) {
        for (term, with_positions) in &self.exact_postings {
            if let Some(full_positions) = self.full_posting_fields.get_mut(&term.field()) {
                *full_positions |= *with_positions;
            }
        }
        self.exact_postings
            .retain(|term, _| !self.full_posting_fields.contains_key(&term.field()));
        self.dictionary_fields
            .retain(|field| !self.full_posting_fields.contains_key(field));
    }

    pub(crate) async fn warm_searcher(&self, searcher: &Searcher, has_score: bool) -> Result<()> {
        // Fast fields are a single segment component. Keep the full file pinned for the
        // synchronous range/exists scorer, irrespective of the exact JSON path.
        let warm_fast_fields = async {
            if self
                .fast_fields
                .iter()
                .any(|field| searcher.schema().get_field_entry(*field).is_fast())
            {
                let segments = searcher.index().searchable_segments()?;
                let files = segments
                    .iter()
                    .map(|segment| segment.open_read(SegmentComponent::FastFields))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                future::try_join_all(files.iter().map(|file| file.read_bytes_async())).await?;
            }
            Ok::<(), databend_common_exception::ErrorCode>(())
        };

        let warm_postings = async {
            // Databend bundles contain one segment, but keep the directory warmup valid for
            // searchers with multiple segments as well.
            future::try_join_all(
                searcher
                    .segment_readers()
                    .iter()
                    .map(|segment| self.warm_segment(segment, has_score)),
            )
            .await?;
            Ok::<(), databend_common_exception::ErrorCode>(())
        };

        tokio::try_join!(warm_fast_fields, warm_postings)?;
        Ok(())
    }

    async fn warm_segment(
        &self,
        segment_reader: &tantivy::SegmentReader,
        has_score: bool,
    ) -> Result<()> {
        let warm_full = async {
            let warmups =
                self.full_posting_fields
                    .iter()
                    .map(|(&field, &with_positions)| async move {
                        let inverted_index = segment_reader.inverted_index(field)?;
                        inverted_index.terms().warm_up_dictionary().await?;
                        inverted_index.warm_postings_full(with_positions).await?;
                        Ok::<(), databend_common_exception::ErrorCode>(())
                    });
            future::try_join_all(warmups).await?;
            Ok::<(), databend_common_exception::ErrorCode>(())
        };

        let warm_dictionaries = async {
            let warmups = self.dictionary_fields.iter().map(|&field| async move {
                segment_reader
                    .inverted_index(field)?
                    .terms()
                    .warm_up_dictionary()
                    .await?;
                Ok::<(), databend_common_exception::ErrorCode>(())
            });
            future::try_join_all(warmups).await?;
            Ok::<(), databend_common_exception::ErrorCode>(())
        };

        let warm_terms = async {
            let mut warmups = Vec::with_capacity(self.exact_postings.len());
            for (term, &with_positions) in &self.exact_postings {
                let inverted_index = segment_reader.inverted_index(term.field())?;
                warmups
                    .push(async move { inverted_index.warm_postings(term, with_positions).await });
            }
            future::try_join_all(warmups).await?;
            Ok::<(), databend_common_exception::ErrorCode>(())
        };

        let warm_fieldnorms = async {
            if has_score {
                let mut fields = self
                    .full_posting_fields
                    .keys()
                    .copied()
                    .collect::<HashSet<_>>();
                fields.extend(self.exact_postings.keys().map(Term::field));
                let fieldnorms = segment_reader.fieldnorms_readers().get_inner_file();
                let files = fields
                    .into_iter()
                    .filter_map(|field| fieldnorms.open_read(field))
                    .collect::<Vec<_>>();
                future::try_join_all(files.iter().map(|file| file.read_bytes_async())).await?;
            }
            Ok::<(), databend_common_exception::ErrorCode>(())
        };

        tokio::try_join!(warm_full, warm_dictionaries, warm_terms, warm_fieldnorms)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tantivy::query::QueryParser;
    use tantivy::query_grammar::parse_query;
    use tantivy::schema::FAST;
    use tantivy::schema::JsonObjectOptions;
    use tantivy::schema::Schema;
    use tantivy::schema::TEXT;
    use tantivy::schema::TextFieldIndexing;
    use tantivy::tokenizer::TokenizerManager;

    use super::*;

    fn plan(text: &str, fuzziness: Option<u8>) -> (InvertedIndexWarmupInfo, Field) {
        let mut builder = Schema::builder();
        let field = builder.add_text_field("body", TEXT);
        let schema = builder.build();
        let tokenizers = TokenizerManager::default();
        let mut parser = QueryParser::new(schema.clone(), vec![field], tokenizers);
        parser.allow_regexes();
        if let Some(distance) = fuzziness {
            parser.set_field_fuzzy(field, false, distance, true);
        }
        let query = parser.parse_query(text).unwrap();
        let ast = parse_query(text).unwrap();
        let plan =
            InvertedIndexWarmupInfo::try_create(query.as_ref(), &ast, &schema, &[field], fuzziness)
                .unwrap();
        (plan, field)
    }

    #[test]
    fn exact_terms_and_boost_do_not_warm_full_postings() {
        for text in ["body:alpha", "body:alpha^2", "body:\"alpha beta\"~1"] {
            let (plan, field) = plan(text, None);
            assert!(plan.full_posting_fields.is_empty());
            assert_eq!(
                plan.exact_postings.len(),
                if text.contains("beta") { 2 } else { 1 }
            );
            if text.contains("beta") {
                assert!(plan.exact_postings.values().all(|&positions| positions));
            } else {
                assert!(!plan.exact_postings[&Term::from_field_text(field, "alpha")]);
            }
        }
    }

    #[test]
    fn expansion_queries_warm_full_postings() {
        for (text, fuzziness, with_positions) in [
            ("body:alpha", Some(1), false),
            ("body:/alp.*/", None, false),
            ("body:[alpha TO delta]", None, false),
            ("body:\"alpha bet\"*", None, true),
        ] {
            let (plan, field) = plan(text, fuzziness);
            assert_eq!(
                plan.full_posting_fields.get(&field),
                Some(&with_positions),
                "{text}"
            );
            assert!(plan.exact_postings.is_empty(), "{text}");
        }
    }

    #[test]
    fn full_postings_absorb_phrase_positions() {
        let (plan, field) = plan("body:\"alpha beta\" OR body:/alp.*/", None);
        assert_eq!(plan.full_posting_fields.get(&field), Some(&true));
        assert!(plan.exact_postings.is_empty());
    }

    #[test]
    fn json_range_only_warms_fast_fields() {
        let mut builder = Schema::builder();
        let field = builder.add_json_field("meta", JsonObjectOptions::default().set_fast(None));
        let schema = builder.build();
        let query_text = "meta.n:[1 TO 9]";
        let ast = parse_query(query_text).unwrap();
        let parser = QueryParser::new(schema.clone(), vec![field], TokenizerManager::default());
        let query = parser.parse_query(query_text).unwrap();
        let plan =
            InvertedIndexWarmupInfo::try_create(query.as_ref(), &ast, &schema, &[field], None)
                .unwrap();
        assert!(plan.fast_fields.contains(&field));
        assert!(plan.full_posting_fields.is_empty());
    }

    #[test]
    fn fast_only_range_does_not_warm_postings() {
        let mut builder = Schema::builder();
        let field = builder.add_u64_field("num", FAST);
        let schema = builder.build();
        let query_text = "num:[1 TO 9]";
        let parser = QueryParser::new(schema.clone(), vec![field], TokenizerManager::default());
        let query = parser.parse_query(query_text).unwrap();
        let ast = parse_query(query_text).unwrap();
        let plan =
            InvertedIndexWarmupInfo::try_create(query.as_ref(), &ast, &schema, &[field], None)
                .unwrap();
        assert!(plan.fast_fields.contains(&field));
        assert!(plan.full_posting_fields.is_empty());
        assert!(plan.dictionary_fields.is_empty());
    }

    #[test]
    fn non_fast_range_warms_full_postings() {
        let (plan, field) = plan("body:[alpha TO delta]", None);
        assert_eq!(plan.full_posting_fields.get(&field), Some(&false));
        assert!(plan.fast_fields.is_empty());
    }

    #[test]
    fn unknown_field_does_not_skip_json_path_fallback() {
        let mut builder = Schema::builder();
        let body = builder.add_text_field("body", TEXT);
        let meta = builder.add_json_field(
            "meta",
            JsonObjectOptions::default()
                .set_indexing_options(TextFieldIndexing::default())
                .set_fast(None),
        );
        let fast_only = builder.add_u64_field("num", FAST);
        let schema = builder.build();
        let ast = parse_query("unknown:/alp.*/").unwrap();
        let plan = InvertedIndexWarmupInfo::try_create(
            &tantivy::query::AllQuery,
            &ast,
            &schema,
            &[body],
            None,
        )
        .unwrap();
        assert!(plan.full_posting_fields.contains_key(&body));
        assert!(plan.full_posting_fields.contains_key(&meta));
        assert!(!plan.full_posting_fields.contains_key(&fast_only));
    }
}
