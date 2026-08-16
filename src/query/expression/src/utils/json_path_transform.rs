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

//! Helpers for the `json_path_transform` lambda function.
//!
//! [`select_locations`] mirrors the lax-mode semantics of
//! `jsonb::jsonpath::Selector` (which backs `json_path_query`) and delegates
//! filter expressions to it, so predicate semantics cannot diverge.

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use jsonb::RawJsonb;
use jsonb::Value as JsonbValue;
use jsonb::from_slice;
use jsonb::jsonpath::JsonPath;
use jsonb::jsonpath::Path;
use jsonb::jsonpath::RecursiveLevel;

/// One structural step from a JSONB node to one of its children.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PathStep {
    Key(String),
    Index(usize),
}

/// A value matched by [`select_locations`]: its structural location relative
/// to the root and the value re-encoded as a standalone JSONB binary.
#[derive(Debug)]
pub struct JsonPathMatch {
    pub location: Vec<PathStep>,
    pub value: Vec<u8>,
}

/// Selects the locations of all values matched by `json_path` inside the
/// JSONB binary `data`, in document order.
///
/// Errors if the path matches nested or duplicate values, which cannot be
/// replaced unambiguously.
pub fn select_locations(data: &[u8], json_path: &JsonPath<'_>) -> Result<Vec<JsonPathMatch>> {
    // Mirrors `Selector::select_by_paths`: a leading `@` is invalid.
    if let Some(Path::Current) = json_path.paths.first() {
        return Err(ErrorCode::BadArguments(
            "json path for json_path_transform must not start with '@'",
        ));
    }

    let root = from_slice(data)
        .map_err(|e| ErrorCode::BadArguments(format!("invalid jsonb value: {e:?}")))?;

    // Candidate nodes after each applied path step, in document order.
    let mut items: Vec<(Vec<PathStep>, &JsonbValue<'_>)> = vec![(vec![], &root)];

    for path in json_path.paths.iter() {
        match path {
            Path::Root | Path::Current => {
                continue;
            }
            Path::FilterExpr(expr) => {
                let filter_path = JsonPath {
                    paths: vec![Path::Root, Path::FilterExpr(expr.clone())],
                };
                let mut kept = Vec::with_capacity(items.len());
                for (loc, node) in items {
                    if eval_filter_on_node(node, &filter_path)? {
                        kept.push((loc, node));
                    }
                }
                items = kept;
            }
            Path::Expr(_) => {
                return Err(ErrorCode::BadArguments(
                    "json_path_transform requires a path that selects document values; \
                     computed JSON path expressions are not supported",
                ));
            }
            Path::RecursiveDotWildcard(level) => {
                let mut next = Vec::with_capacity(items.len());
                for (loc, node) in items {
                    collect_recursive_locations(loc, node, 0, level, &mut next);
                }
                items = next;
            }
            Path::DotWildcard => {
                let mut next = Vec::with_capacity(items.len());
                for (loc, node) in items {
                    if let JsonbValue::Object(obj) = node {
                        for (key, val) in obj.iter() {
                            let mut child_loc = loc.clone();
                            child_loc.push(PathStep::Key(key.clone()));
                            next.push((child_loc, val));
                        }
                    }
                }
                items = next;
            }
            Path::DotField(name) | Path::ColonField(name) | Path::ObjectField(name) => {
                let mut next = Vec::with_capacity(items.len());
                for (loc, node) in items {
                    if let JsonbValue::Object(obj) = node {
                        if let Some(val) = obj.get(name.as_ref()) {
                            let mut child_loc = loc;
                            child_loc.push(PathStep::Key(name.to_string()));
                            next.push((child_loc, val));
                        }
                    }
                }
                items = next;
            }
            Path::BracketWildcard => {
                let mut next = Vec::with_capacity(items.len());
                for (loc, node) in items {
                    if let JsonbValue::Array(arr) = node {
                        for (i, val) in arr.iter().enumerate() {
                            let mut child_loc = loc.clone();
                            child_loc.push(PathStep::Index(i));
                            next.push((child_loc, val));
                        }
                    } else {
                        // Lax mode: a bracket wildcard on a non-array
                        // auto-wraps the value, keeping the node itself.
                        next.push((loc, node));
                    }
                }
                items = next;
            }
            Path::ArrayIndices(array_indices) => {
                let mut next = Vec::with_capacity(items.len());
                for (loc, node) in items {
                    let JsonbValue::Array(arr) = node else {
                        continue;
                    };
                    for array_index in array_indices {
                        let indices = array_index.to_indices(arr.len());
                        if indices.is_empty() {
                            continue;
                        }
                        for (i, val) in arr.iter().enumerate() {
                            if indices.contains(&i) {
                                let mut child_loc = loc.clone();
                                child_loc.push(PathStep::Index(i));
                                next.push((child_loc, val));
                            }
                        }
                    }
                }
                items = next;
            }
        }
    }

    check_no_overlap(&items)?;

    Ok(items
        .into_iter()
        .map(|(location, node)| JsonPathMatch {
            location,
            value: node.to_vec(),
        })
        .collect())
}

fn collect_recursive_locations<'data, 'node>(
    location: Vec<PathStep>,
    node: &'node JsonbValue<'data>,
    current_level: u8,
    recursive_level: &Option<RecursiveLevel>,
    out: &mut Vec<(Vec<PathStep>, &'node JsonbValue<'data>)>,
) {
    let (is_match, should_continue) = match recursive_level {
        Some(level) => level.check_recursive_level(current_level),
        None => (true, true),
    };
    if is_match {
        out.push((location.clone(), node));
    }
    if !should_continue {
        return;
    }

    match node {
        JsonbValue::Object(obj) => {
            for (key, child) in obj {
                let mut child_location = location.clone();
                child_location.push(PathStep::Key(key.clone()));
                collect_recursive_locations(
                    child_location,
                    child,
                    current_level + 1,
                    recursive_level,
                    out,
                );
            }
        }
        JsonbValue::Array(arr) => {
            for (index, child) in arr.iter().enumerate() {
                let mut child_location = location.clone();
                child_location.push(PathStep::Index(index));
                collect_recursive_locations(
                    child_location,
                    child,
                    current_level + 1,
                    recursive_level,
                    out,
                );
            }
        }
        _ => {}
    }
}

/// Evaluates a filter path (`$ ? (expr)`) against a single node with the
/// jsonb selector, keeping predicate semantics identical to `json_path_query`.
fn eval_filter_on_node(node: &JsonbValue<'_>, filter_path: &JsonPath<'_>) -> Result<bool> {
    let bytes = node.to_vec();
    let raw = RawJsonb::new(&bytes);
    match raw.select_by_path(filter_path) {
        Ok(values) => Ok(!values.is_empty()),
        Err(e) => Err(ErrorCode::BadArguments(format!(
            "failed to evaluate json path filter: {e:?}"
        ))),
    }
}

fn check_no_overlap(items: &[(Vec<PathStep>, &JsonbValue<'_>)]) -> Result<()> {
    if items.len() < 2 {
        return Ok(());
    }
    let mut locations: Vec<&Vec<PathStep>> = items.iter().map(|(loc, _)| loc).collect();
    locations.sort();
    for pair in locations.windows(2) {
        if pair[1].starts_with(pair[0]) {
            return Err(ErrorCode::BadArguments(
                "json path for json_path_transform matched nested or duplicate \
                 values, which cannot be replaced unambiguously",
            ));
        }
    }
    Ok(())
}

/// Rebuilds the JSONB binary `data` with the values at `locations`
/// replaced by `replacements` (`None` writes a JSON null). Everything
/// outside the replaced locations is preserved.
pub fn replace_at_locations<'a>(
    data: &'a [u8],
    locations: &[Vec<PathStep>],
    replacements: &[Option<&'a [u8]>],
) -> Result<Vec<u8>> {
    debug_assert_eq!(locations.len(), replacements.len());

    let mut root = from_slice(data)
        .map_err(|e| ErrorCode::BadArguments(format!("invalid jsonb value: {e:?}")))?;
    for (location, replacement) in locations.iter().zip(replacements.iter()) {
        let new_value = match replacement {
            Some(bytes) => from_slice(bytes).map_err(|e| {
                ErrorCode::BadArguments(format!("invalid jsonb replacement value: {e:?}"))
            })?,
            None => JsonbValue::Null,
        };
        set_at_location(&mut root, location, new_value)?;
    }
    Ok(root.to_vec())
}

fn set_at_location<'a>(
    root: &mut JsonbValue<'a>,
    location: &[PathStep],
    new_value: JsonbValue<'a>,
) -> Result<()> {
    let mut cur = root;
    for step in location {
        cur = match (cur, step) {
            (JsonbValue::Object(obj), PathStep::Key(key)) => obj.get_mut(key).ok_or_else(|| {
                ErrorCode::Internal("json_path_transform: matched location disappeared")
            })?,
            (JsonbValue::Array(arr), PathStep::Index(i)) => arr.get_mut(*i).ok_or_else(|| {
                ErrorCode::Internal("json_path_transform: matched location disappeared")
            })?,
            _ => {
                return Err(ErrorCode::Internal(
                    "json_path_transform: matched location has unexpected shape",
                ));
            }
        };
    }
    *cur = new_value;
    Ok(())
}

#[cfg(test)]
mod tests {
    use jsonb::jsonpath::parse_json_path;
    use jsonb::parse_value;

    use super::*;

    fn jsonb_of(text: &str) -> Vec<u8> {
        parse_value(text.as_bytes()).unwrap().to_vec()
    }

    fn matched_values(data: &[u8], path: &str) -> Vec<String> {
        let json_path = parse_json_path(path.as_bytes()).unwrap();
        select_locations(data, &json_path)
            .unwrap()
            .into_iter()
            .map(|m| RawJsonb::new(&m.value).to_string())
            .collect()
    }

    /// The walker must select the same values as the jsonb selector.
    fn assert_matches_selector(data: &[u8], path: &str) {
        let json_path = parse_json_path(path.as_bytes()).unwrap();
        let ours: Vec<String> = select_locations(data, &json_path)
            .unwrap()
            .into_iter()
            .map(|m| RawJsonb::new(&m.value).to_string())
            .collect();
        let mut ours_sorted = ours.clone();
        ours_sorted.sort();
        let mut selector: Vec<String> = RawJsonb::new(data)
            .select_by_path(&json_path)
            .unwrap()
            .into_iter()
            .map(|v| v.as_raw().to_string())
            .collect();
        selector.sort();
        assert_eq!(ours_sorted, selector, "path {path} diverged from selector");
    }

    #[test]
    fn test_select_locations_matches_selector() {
        let doc = jsonb_of(
            r#"[
              {"name":"ev1","attributes":[
                {"key":"body","value":{"stringValue":"s1"}},
                {"key":"other","value":{"intValue":1}}]},
              {"name":"ev2","attributes":[
                {"key":"body","value":{"otherField":true}},
                {"key":"body","value":{"stringValue":null}}]},
              {"name":"ev3"}
            ]"#,
        );
        for path in [
            "$",
            "$[*]",
            "$[0]",
            "$[1 to last]",
            "$[*].name",
            "$[*].attributes",
            "$[*].attributes[*]",
            "$[*].attributes[*].key",
            r#"$[*].attributes[*] ? (@.key == "body")"#,
            r#"$[*].attributes[*] ? (@.key == "body").value.stringValue"#,
            r#"$[*].attributes[*] ? (@.key == "missing")"#,
            "$[*].missing",
            "$.*",
        ] {
            assert_matches_selector(&doc, path);
        }

        // Lax-mode auto-wrap of bracket wildcard on non-arrays.
        for doc in [jsonb_of(r#"{"a":1}"#), jsonb_of("null"), jsonb_of("123")] {
            assert_matches_selector(&doc, "$[*]");
            assert_matches_selector(&doc, "$[*].a");
        }
    }

    #[test]
    fn test_select_locations_document_order() {
        let doc = jsonb_of(
            r#"{"content":[{"type":"text","text":"a"},{"type":"image"},{"type":"text","text":"b"}]}"#,
        );
        let values = matched_values(&doc, r#"$.content[*] ? (@.type == "text").text"#);
        assert_eq!(values, vec![r#""a""#, r#""b""#]);
    }

    #[test]
    fn test_recursive_and_predicate_paths_match_selector() {
        let doc = jsonb_of(r#"{"a":{"b":1},"c":2}"#);
        let level_one = format!("$.**{}1{}", '{', '}');
        let level_two = format!("$.**{}2{}", '{', '}');
        for path in [&level_one, &level_two] {
            assert_matches_selector(&doc, path);
        }

        // A computed expression yields derived values, not document locations.
        let computed_path = parse_json_path(b"$.c == 2").unwrap();
        assert!(select_locations(&doc, &computed_path).is_err());

        // `$.**` selects ancestors and descendants: ambiguous for replacement.
        let json_path = parse_json_path(b"$.**").unwrap();
        assert!(select_locations(&doc, &json_path).is_err());
    }

    #[test]
    fn test_duplicate_match_rejected() {
        let doc = jsonb_of("[1, 2, 3]");
        let json_path = parse_json_path(b"$[0, 0]").unwrap();
        assert!(select_locations(&doc, &json_path).is_err());
    }

    #[test]
    fn test_replace_at_locations() {
        let doc = jsonb_of(
            r#"{"events":[
                {"key":"body","value":"long text"},
                {"key":"other","value":42}
            ]}"#,
        );
        let json_path = parse_json_path(br#"$.events[*] ? (@.key == "body").value"#).unwrap();
        let matches = select_locations(&doc, &json_path).unwrap();
        assert_eq!(matches.len(), 1);

        let replacement = jsonb_of(r#""short""#);
        let locations: Vec<_> = matches.iter().map(|m| m.location.clone()).collect();
        let out = replace_at_locations(&doc, &locations, &[Some(replacement.as_slice())]).unwrap();
        assert_eq!(
            RawJsonb::new(&out).to_string(),
            r#"{"events":[{"key":"body","value":"short"},{"key":"other","value":42}]}"#
        );

        // `None` writes a JSON null.
        let out = replace_at_locations(&doc, &locations, &[None]).unwrap();
        assert_eq!(
            RawJsonb::new(&out).to_string(),
            r#"{"events":[{"key":"body","value":null},{"key":"other","value":42}]}"#
        );
    }

    #[test]
    fn test_replace_root() {
        let doc = jsonb_of(r#"{"a":1}"#);
        let json_path = parse_json_path(b"$").unwrap();
        let matches = select_locations(&doc, &json_path).unwrap();
        assert_eq!(matches.len(), 1);
        assert!(matches[0].location.is_empty());

        let replacement = jsonb_of("[true]");
        let out = replace_at_locations(&doc, &[matches[0].location.clone()], &[Some(
            replacement.as_slice(),
        )])
        .unwrap();
        assert_eq!(RawJsonb::new(&out).to_string(), "[true]");
    }

    #[test]
    fn test_no_match_roundtrip_unchanged() {
        let doc = jsonb_of(r#"{"a":[1,{"b":2}],"c":"x"}"#);
        let json_path = parse_json_path(b"$.missing").unwrap();
        let matches = select_locations(&doc, &json_path).unwrap();
        assert!(matches.is_empty());
        // Untouched parts of a rebuilt document rely on roundtrip fidelity.
        let root = from_slice(&doc).unwrap();
        assert_eq!(root.to_vec(), doc);
    }
}
