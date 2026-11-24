// Copyright 2026 Datafuse Labs
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

use databend_common_ast::ast::SampleConfig;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_expression::DataSchema;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SEARCH_MATCHED_COL_NAME;
use databend_common_expression::SEARCH_SCORE_COL_NAME;
use databend_common_expression::Scalar;
use databend_common_expression::VECTOR_SCORE_COL_NAME;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_storages_common_table_meta::table::ChangeType;

fn column(name: &str, data_type: DataType) -> RemoteExpr<String> {
    RemoteExpr::ColumnRef {
        span: None,
        id: name.to_string(),
        data_type,
        display_name: name.to_string(),
    }
}

fn score_top_k() -> PushDownInfo {
    PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: "fts".to_string(),
            index_version: "v1".to_string(),
            index_options: Default::default(),
            index_schema: DataSchema::empty(),
            query_fields: vec![("body".to_string(), None)],
            query_text: "hello".to_string(),
            has_score: true,
            inverted_index_option: None,
        }),
        filters: Some(Filters {
            filter: column(SEARCH_MATCHED_COL_NAME, DataType::Boolean),
            inverted_filter: RemoteExpr::Constant {
                span: None,
                scalar: Scalar::Boolean(false),
                data_type: DataType::Boolean,
            },
        }),
        is_deterministic: true,
        order_by: vec![(
            column(
                SEARCH_SCORE_COL_NAME,
                DataType::Number(NumberDataType::Float32),
            ),
            false,
            false,
        )],
        limit: Some(10),
        ..Default::default()
    }
}

#[test]
fn test_inverted_index_top_k_bounds_and_serialization() {
    let mut push_down = score_top_k();
    for limit in [1, 10, 50, usize::MAX] {
        push_down.limit = Some(limit);
        for nulls_first in [false, true] {
            // FTS scores are non-nullable, so NULLS FIRST does not change ranking.
            push_down.order_by[0].2 = nulls_first;
            assert_eq!(push_down.inverted_index_top_k(), Some(limit));
            let encoded = serde_json::to_string(&push_down).unwrap();
            let decoded: PushDownInfo = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded.inverted_index_top_k(), Some(limit));
        }
    }
}

#[test]
fn test_inverted_index_top_k_rejects_unsafe_pushdowns() {
    fn rejects(change: impl FnOnce(&mut PushDownInfo)) {
        let mut push_down = score_top_k();
        change(&mut push_down);
        assert_eq!(push_down.inverted_index_top_k(), None, "{push_down:?}");
    }

    rejects(|p| p.limit = None);
    rejects(|p| p.limit = Some(0));
    rejects(|p| p.inverted_index = None);
    rejects(|p| p.inverted_index.as_mut().unwrap().has_score = false);
    rejects(|p| p.is_deterministic = false);
    rejects(|p| p.filters = None);
    rejects(|p| {
        p.filters.as_mut().unwrap().filter = column("tenant_id", DataType::Boolean);
    });
    rejects(|p| {
        p.filters.as_mut().unwrap().filter = RemoteExpr::Cast {
            span: None,
            is_try: false,
            expr: Box::new(column(SEARCH_MATCHED_COL_NAME, DataType::Boolean)),
            dest_type: DataType::Boolean,
        };
    });
    rejects(|p| p.secure_filters = p.filters.clone());
    rejects(|p| {
        p.prewhere = Some(PrewhereInfo {
            output_columns: Projection::Columns(vec![]),
            prewhere_columns: Projection::Columns(vec![]),
            remain_columns: Projection::Columns(vec![]),
            filter: p.filters.as_ref().unwrap().filter.clone(),
            virtual_column_ids: None,
        });
    });
    rejects(|p| p.sample = Some(SampleConfig::default()));
    rejects(|p| p.change_type = Some(ChangeType::Append));
    rejects(|p| {
        p.vector_index = Some(VectorIndexInfo {
            index_name: "vector".to_string(),
            index_version: "v1".to_string(),
            index_options: Default::default(),
            column_id: 0,
            func_name: "l2_distance".to_string(),
            query_values: vec![],
        });
    });
    rejects(|p| p.order_by.clear());
    rejects(|p| p.order_by[0].1 = true);
    rejects(|p| p.order_by.push(p.order_by[0].clone()));
    rejects(|p| p.order_by[0].0 = column("id", DataType::Number(NumberDataType::UInt64)));
    rejects(|p| p.order_by[0].0 = column(SEARCH_SCORE_COL_NAME, DataType::String));
    rejects(|p| {
        p.order_by[0].0 = RemoteExpr::Cast {
            span: None,
            is_try: false,
            expr: Box::new(p.order_by[0].0.clone()),
            dest_type: DataType::Number(NumberDataType::Float64),
        };
    });
}

fn vector_top_n() -> PushDownInfo {
    PushDownInfo {
        vector_index: Some(VectorIndexInfo {
            index_name: "vector".to_string(),
            index_version: "v1".to_string(),
            index_options: Default::default(),
            column_id: 0,
            func_name: "l2_distance".to_string(),
            query_values: vec![],
        }),
        order_by: vec![(
            column(
                VECTOR_SCORE_COL_NAME,
                DataType::Number(NumberDataType::Float32),
            ),
            true,
            false,
        )],
        limit: Some(10),
        ..Default::default()
    }
}

#[test]
fn test_index_top_k_accepts_covering_scans() {
    assert!(score_top_k().index_top_k_score().is_some());
    let mut push_down = vector_top_n();
    assert!(push_down.index_top_k_score().is_some());
    // Only specialize ordering by the indexed score alone.
    push_down.order_by.push((
        column("id", DataType::Number(NumberDataType::UInt64)),
        true,
        false,
    ));
    assert!(push_down.index_top_k_score().is_none());
}

#[test]
fn test_index_top_k_rejects_scans_the_index_cannot_answer() {
    fn rejects(change: impl FnOnce(&mut PushDownInfo)) {
        let mut push_down = vector_top_n();
        change(&mut push_down);
        assert!(push_down.index_top_k_score().is_none(), "{push_down:?}");
    }

    assert!(PushDownInfo::default().index_top_k_score().is_none());
    rejects(|p| p.vector_index = None);
    rejects(|p| p.limit = None);
    rejects(|p| p.limit = Some(0));
    rejects(|p| p.order_by.clear());
    // Farthest rows: every row is scored, so the pruned rows are not covering.
    rejects(|p| p.order_by[0].1 = false);
    rejects(|p| p.order_by[0].0 = column("id", DataType::Number(NumberDataType::UInt64)));
    rejects(|p| p.filters = score_top_k().filters);
    rejects(|p| p.inverted_index = score_top_k().inverted_index);
}

/// The planner replaces the scan and TopN by the index matches under
/// `covering_index_score`; a lazy reader skips Parquet only under the narrower
/// `index_top_k_score`, which also needs the index cut to a Top-K. Both rules
/// share the same exclusions, so the two must differ only by the Top-K shape.
#[test]
fn test_covering_index_score_contains_index_top_k() {
    use InternalColumnType::SearchScore;
    use InternalColumnType::VectorScore;

    fn with(mut push_down: PushDownInfo, change: impl FnOnce(&mut PushDownInfo)) -> PushDownInfo {
        change(&mut push_down);
        push_down
    }
    let id = || column("id", DataType::Number(NumberDataType::UInt64));

    for (case, push_down, covering, top_k) in [
        (
            "fts desc",
            score_top_k(),
            Some(SearchScore),
            Some(SearchScore),
        ),
        // Without a Top-K, FTS pruning keeps every match of the block.
        (
            "fts asc",
            with(score_top_k(), |p| p.order_by[0].1 = true),
            Some(SearchScore),
            None,
        ),
        (
            "fts without limit",
            with(score_top_k(), |p| p.limit = None),
            Some(SearchScore),
            None,
        ),
        (
            "fts with a second sort key",
            with(score_top_k(), |p| p.order_by.push((id(), true, false))),
            Some(SearchScore),
            None,
        ),
        (
            "vector asc",
            vector_top_n(),
            Some(VectorScore),
            Some(VectorScore),
        ),
        // Every row is scored: blocks are not `rows_only`, so none is covered at run time.
        (
            "vector desc",
            with(vector_top_n(), |p| p.order_by[0].1 = false),
            Some(VectorScore),
            None,
        ),
        (
            "vector without limit",
            with(vector_top_n(), |p| p.limit = None),
            Some(VectorScore),
            None,
        ),
        (
            "fts with a residual filter",
            with(score_top_k(), |p| {
                p.filters.as_mut().unwrap().filter = column("tenant_id", DataType::Boolean);
            }),
            None,
            None,
        ),
        (
            "vector with a filter",
            with(vector_top_n(), |p| p.filters = score_top_k().filters),
            None,
            None,
        ),
        (
            "both indexes",
            with(score_top_k(), |p| {
                p.vector_index = vector_top_n().vector_index
            }),
            None,
            None,
        ),
        ("no index", PushDownInfo::default(), None, None),
    ] {
        assert_eq!(push_down.covering_index_score(), covering, "{case}");
        assert_eq!(push_down.index_top_k_score(), top_k, "{case}");
    }

    let exclusions: [(&str, fn(&mut PushDownInfo)); 4] = [
        ("prewhere", |p| {
            p.prewhere = Some(PrewhereInfo {
                output_columns: Projection::Columns(vec![]),
                prewhere_columns: Projection::Columns(vec![]),
                remain_columns: Projection::Columns(vec![]),
                filter: column(SEARCH_MATCHED_COL_NAME, DataType::Boolean),
                virtual_column_ids: None,
            });
        }),
        ("secure filters", |p| {
            p.secure_filters = score_top_k().filters
        }),
        ("sample", |p| p.sample = Some(SampleConfig::default())),
        ("change type", |p| p.change_type = Some(ChangeType::Append)),
    ];
    for (case, exclude) in exclusions {
        for push_down in [score_top_k(), vector_top_n()] {
            let push_down = with(push_down, exclude);
            assert_eq!(push_down.covering_index_score(), None, "{case}");
            assert_eq!(push_down.index_top_k_score(), None, "{case}");
        }
    }
}
