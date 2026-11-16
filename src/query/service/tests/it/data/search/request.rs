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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_query::data::search::RetrieverSpec;
use databend_query::data::search::SearchRequest;
use databend_query::data::search::TextOperator;
use databend_query::data::search::VectorDistance;
use databend_query::data::search::retrievers::text::parse_boosted_column;

fn request(json: &str) -> Result<SearchRequest> {
    let request: SearchRequest =
        serde_json::from_str(json).map_err(|e| ErrorCode::BadArguments(e.to_string()))?;
    request.validate()?;
    Ok(request)
}

#[test]
fn test_text_request() {
    let r = request(
        r#"{"database": "fts", "table": "docs", "limit": 10, "output": ["id", "_score"],
            "retrieve": {"text": {"columns": ["title^2", "body"], "query": "rust db"}}}"#,
    )
    .unwrap();
    assert_eq!(r.catalog, None);
    assert_eq!(r.database.as_deref(), Some("fts"));
    assert_eq!(r.candidates(), 10);
    let RetrieverSpec::Text(spec) = &r.retrieve else {
        panic!("not a text search: {r:?}");
    };
    assert_eq!(spec.operator, TextOperator::Or);
    assert!(!spec.lenient);
    assert_eq!(
        parse_boosted_column(&spec.columns[0]).unwrap(),
        ("title", Some(2.0))
    );
    assert_eq!(
        parse_boosted_column(&spec.columns[1]).unwrap(),
        ("body", None)
    );
}

#[test]
fn test_vector_request() {
    let r = request(
        r#"{"table": "docs", "limit": 5, "offset": 2, "output": ["id"],
            "retrieve": {"vector": {"column": "v", "query": [0.5, -1, 2], "distance": "cosine"}}}"#,
    )
    .unwrap();
    assert_eq!(r.candidates(), 7);
    let RetrieverSpec::Vector(spec) = &r.retrieve else {
        panic!("not a vector search: {r:?}");
    };
    assert_eq!(spec.column, "v");
    assert_eq!(spec.query, vec![0.5, -1.0, 2.0]);
    assert_eq!(spec.distance, VectorDistance::Cosine);
    assert_eq!(spec.distance.func_name(), "cosine_distance");
}

#[test]
fn test_invalid_requests() {
    let text = r#""retrieve": {"text": {"columns": ["body"], "query": "q"}}"#;
    for json in [
        format!(r#"{{"table": "t", "limit": 0, "output": ["id"], {text}}}"#),
        format!(r#"{{"table": "t", "limit": 10001, "output": ["id"], {text}}}"#),
        format!(r#"{{"table": "t", "limit": 1, "offset": 10000, "output": ["id"], {text}}}"#),
        format!(r#"{{"table": "t", "limit": 1, "output": [], {text}}}"#),
        format!(r#"{{"table": "t", "limit": 1, "output": ["id", "id"], {text}}}"#),
        format!(r#"{{"table": "t", "limit": 1, "output": ["id"], "where": 1, {text}}}"#),
        r#"{"table": "t", "limit": 1, "output": ["id"], "retrieve": {"text": {"columns": [], "query": "q"}}}"#.to_string(),
        r#"{"table": "t", "limit": 1, "output": ["id"], "retrieve": {"text": {"columns": ["b^x"], "query": "q"}}}"#.to_string(),
        r#"{"table": "t", "limit": 1, "output": ["id"], "retrieve": {"vector": {}}}"#.to_string(),
        r#"{"table": "t", "limit": 1, "output": ["id"], "retrieve": {"text": {"columns": ["b"], "query": "q", "fuzziness": 3}}}"#.to_string(),
        r#"{"table": "t", "limit": 1, "output": ["id"], "retrieve": {"vector": {"column": "v", "query": [], "distance": "l2"}}}"#.to_string(),
        r#"{"table": "t", "limit": 1, "output": ["id"], "retrieve": {"vector": {"column": "v", "query": [1.0], "distance": "dot"}}}"#.to_string(),
    ] {
        assert!(request(&json).is_err(), "{json}");
    }
}
