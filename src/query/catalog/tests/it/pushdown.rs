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
