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

use databend_common_exception::Result;
use tantivy::Searcher;
use tantivy::Term;
use tantivy::index::SegmentComponent;
use tantivy::query::AllQuery;
use tantivy::query::BooleanQuery;
use tantivy::query::BoostQuery;
use tantivy::query::ConstScoreQuery;
use tantivy::query::EmptyQuery;
use tantivy::query::FuzzyTermQuery;
use tantivy::query::PhrasePrefixQuery;
use tantivy::query::PhraseQuery;
use tantivy::query::Query;
use tantivy::query::RangeQuery;
use tantivy::query::TermQuery;
use tantivy::schema::Field;

#[derive(Clone)]
enum WarmupAction {
    Term { term: Term, with_positions: bool },
    Full { field: Field, with_positions: bool },
    Range { field: Field },
}

/// Query-level plan of the remote Tantivy ranges that must be loaded before synchronous search.
///
/// The plan is built once with the query and can be reused for every block. Fixed-size pages may
/// be reused globally, while each block's `SearchPinDirectory` retains the exact warmed ranges
/// required by Tantivy's subsequent synchronous search.
#[derive(Clone, Default)]
pub struct InvertedIndexWarmupInfo {
    actions: Vec<WarmupAction>,
}

impl InvertedIndexWarmupInfo {
    pub fn try_create(
        query: &dyn Query,
        fallback_fields: &[Field],
        need_position: bool,
    ) -> Result<Self> {
        let mut actions = Vec::new();
        collect_warmups(query, fallback_fields, need_position, &mut actions)?;
        Ok(Self {
            actions: deduplicate_warmups(actions),
        })
    }

    pub(crate) async fn warm_searcher(&self, searcher: &Searcher, has_score: bool) -> Result<()> {
        let mut warmed_fields = self
            .actions
            .iter()
            .filter_map(|action| match action {
                WarmupAction::Term { term, .. } => Some(term.field()),
                WarmupAction::Full { field, .. } => Some(*field),
                WarmupAction::Range { .. } => None,
            })
            .collect::<Vec<_>>();
        warmed_fields.sort_unstable_by_key(|field| field.field_id());
        warmed_fields.dedup();

        // Tantivy's JSON RangeQuery reads the columnar `.fast` component synchronously while
        // building its scorer. Until Tantivy exposes range-specific async fast-field warmup,
        // retain the complete logical file in this query's cache before synchronous search.
        let warm_fast_fields = self.actions.iter().any(|action| {
            let WarmupAction::Range { field } = action else {
                return false;
            };
            searcher.schema().get_field_entry(*field).is_fast()
        });
        if warm_fast_fields {
            for segment in searcher.index().searchable_segments()? {
                segment
                    .open_read(SegmentComponent::FastFields)?
                    .read_bytes_async()
                    .await?;
            }
        }

        for segment_reader in searcher.segment_readers() {
            for action in &self.actions {
                match action {
                    WarmupAction::Term {
                        term,
                        with_positions,
                    } => {
                        segment_reader
                            .inverted_index(term.field())?
                            .warm_postings(term, *with_positions)
                            .await?;
                    }
                    WarmupAction::Full {
                        field,
                        with_positions,
                    } => {
                        let inverted_index = segment_reader.inverted_index(*field)?;
                        inverted_index.terms().warm_up_dictionary().await?;
                        inverted_index.warm_postings_full(*with_positions).await?;
                    }
                    WarmupAction::Range { field } => {
                        if !searcher.schema().get_field_entry(*field).is_fast() {
                            let inverted_index = segment_reader.inverted_index(*field)?;
                            inverted_index.terms().warm_up_dictionary().await?;
                            inverted_index.warm_postings_full(false).await?;
                        }
                    }
                }
            }

            if has_score {
                for field in &warmed_fields {
                    let fieldnorms = segment_reader.fieldnorms_readers().get_inner_file();
                    if let Some(file) = fieldnorms.open_read(*field) {
                        file.read_bytes_async().await?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn collect_warmups(
    query: &dyn Query,
    fallback_fields: &[Field],
    need_position: bool,
    actions: &mut Vec<WarmupAction>,
) -> Result<()> {
    if let Some(term_query) = query.downcast_ref::<TermQuery>() {
        actions.push(WarmupAction::Term {
            term: term_query.term().clone(),
            with_positions: need_position,
        });
    } else if let Some(boolean_query) = query.downcast_ref::<BooleanQuery>() {
        for (_, subquery) in boolean_query.clauses() {
            collect_warmups(subquery.as_ref(), fallback_fields, need_position, actions)?;
        }
    } else if let Some(phrase_query) = query.downcast_ref::<PhraseQuery>() {
        actions.extend(
            phrase_query
                .phrase_terms()
                .into_iter()
                .map(|term| WarmupAction::Term {
                    term,
                    with_positions: true,
                }),
        );
    } else if let Some(range_query) = query.downcast_ref::<RangeQuery>() {
        actions.push(WarmupAction::Range {
            field: range_query.field(),
        });
    } else if query.downcast_ref::<PhrasePrefixQuery>().is_some() {
        // TODO: Replace this paged full-field fallback with Tantivy-native prefix expansion
        // warmup. Databend must not duplicate Tantivy's prefix/range semantics.
        add_full_field_warmups(query, fallback_fields, true, actions);
    } else if query.downcast_ref::<FuzzyTermQuery>().is_some() {
        // TODO: Replace this paged full-field fallback with Tantivy-native automaton warmup,
        // including JSON-path bounds.
        add_full_field_warmups(query, fallback_fields, need_position, actions);
    } else if query.downcast_ref::<BoostQuery>().is_some()
        || query.downcast_ref::<ConstScoreQuery>().is_some()
    {
        // These wrappers do not expose their child query. Query::query_terms is sufficient for an
        // exact-term child, but range and automaton children may expose no terms at all.
        add_query_term_warmups(query, fallback_fields, need_position, actions);
        add_range_warmups(query, fallback_fields, actions);
    } else if query.downcast_ref::<AllQuery>().is_none()
        && query.downcast_ref::<EmptyQuery>().is_none()
    {
        // TODO: Add Tantivy-native recursion for opaque composite queries such as
        // DisjunctionMaxQuery. A hidden range child needs `.fast`, while fuzzy/regex children need
        // postings. Warm both conservative fallbacks so synchronous search cannot miss the query
        // cache.
        add_full_field_warmups(query, fallback_fields, need_position, actions);
        add_range_warmups(query, fallback_fields, actions);
    }
    Ok(())
}

fn add_query_term_warmups(
    query: &dyn Query,
    fallback_fields: &[Field],
    need_position: bool,
    actions: &mut Vec<WarmupAction>,
) {
    let start_len = actions.len();
    query.query_terms(&mut |term, positions| {
        actions.push(WarmupAction::Term {
            term: term.clone(),
            with_positions: need_position || positions,
        });
    });
    if actions.len() == start_len {
        add_full_field_warmups(query, fallback_fields, need_position, actions);
    }
}

fn add_range_warmups(
    query: &dyn Query,
    fallback_fields: &[Field],
    actions: &mut Vec<WarmupAction>,
) {
    let mut fields = query_field_ids(query)
        .into_iter()
        .map(Field::from_field_id)
        .collect::<Vec<_>>();
    if fields.is_empty() {
        fields.extend_from_slice(fallback_fields);
    }
    fields.sort_unstable_by_key(|field| field.field_id());
    fields.dedup();
    actions.extend(
        fields
            .into_iter()
            .map(|field| WarmupAction::Range { field }),
    );
}

fn deduplicate_warmups(actions: Vec<WarmupAction>) -> Vec<WarmupAction> {
    let mut full_fields = std::collections::HashMap::new();
    let mut range_fields = HashSet::new();
    for action in &actions {
        match action {
            WarmupAction::Full {
                field,
                with_positions,
            } => {
                let entry = full_fields.entry(*field).or_insert(false);
                *entry |= *with_positions;
            }
            WarmupAction::Range { field } => {
                range_fields.insert(*field);
            }
            WarmupAction::Term { .. } => {}
        }
    }

    let mut terms = std::collections::HashMap::new();
    for action in actions {
        if let WarmupAction::Term {
            term,
            with_positions,
        } = action
        {
            if let Some(full_with_positions) = full_fields.get_mut(&term.field()) {
                *full_with_positions |= with_positions;
                continue;
            }
            let entry = terms.entry(term).or_insert(false);
            *entry |= with_positions;
        }
    }

    let mut deduplicated = full_fields
        .into_iter()
        .map(|(field, with_positions)| WarmupAction::Full {
            field,
            with_positions,
        })
        .collect::<Vec<_>>();
    deduplicated.extend(
        terms
            .into_iter()
            .map(|(term, with_positions)| WarmupAction::Term {
                term,
                with_positions,
            }),
    );
    deduplicated.extend(
        range_fields
            .into_iter()
            .map(|field| WarmupAction::Range { field }),
    );
    deduplicated.sort_unstable_by_key(|action| match action {
        WarmupAction::Full { field, .. } => (field.field_id(), 0),
        WarmupAction::Term { term, .. } => (term.field().field_id(), 1),
        WarmupAction::Range { field } => (field.field_id(), 2),
    });
    deduplicated
}

fn add_full_field_warmups(
    query: &dyn Query,
    fallback_fields: &[Field],
    with_positions: bool,
    actions: &mut Vec<WarmupAction>,
) {
    let mut fields = query_field_ids(query)
        .into_iter()
        .map(Field::from_field_id)
        .collect::<Vec<_>>();
    if fields.is_empty() {
        fields.extend_from_slice(fallback_fields);
    }
    fields.sort_unstable_by_key(|field| field.field_id());
    fields.dedup();
    actions.extend(fields.into_iter().map(|field| WarmupAction::Full {
        field,
        with_positions,
    }));
}

fn query_field_ids(query: &dyn Query) -> HashSet<u32> {
    let mut field_ids = HashSet::new();
    query.query_terms(&mut |term, _| {
        field_ids.insert(term.field().field_id());
    });
    field_ids
}
