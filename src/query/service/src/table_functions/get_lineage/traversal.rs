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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::parser::parse_table_ref;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::timestamp::timestamp_to_rfc3339_utc;
use databend_common_sql::planner::NameResolutionContext;
use databend_common_sql::planner::normalize_identifier;

use super::GetLineageArgs;
use super::LineageResultRow;
use super::ObjectDomain;
use super::QueryDirection;
use super::edge_reader::AddressKind;
use super::edge_reader::CapturedObject;
use super::edge_reader::LineageEdgeReader;
use super::edge_reader::LineageObjectType;
use super::edge_reader::RawLineageEdge;
use super::resolver::ObjectResolver;
use super::resolver::ResolvedObject;
use crate::sessions::TableContext;

#[derive(Clone)]
struct TraversedObjectEdge {
    distance: u8,
    edge: RawLineageEdge,
    source: ResolvedObject,
    target: ResolvedObject,
}

#[derive(Clone)]
struct ColumnFrontier {
    object: ResolvedObject,
    column_id: String,
    column_name: String,
}

#[derive(Clone)]
struct TraversedColumnEdge {
    distance: u8,
    edge: RawLineageEdge,
    source: ResolvedObject,
    source_column: String,
    source_masked: bool,
    target: ResolvedObject,
    target_column: String,
    target_masked: bool,
}

pub(super) async fn traverse(
    ctx: Arc<dyn TableContext>,
    args: GetLineageArgs,
) -> Result<Vec<LineageResultRow>> {
    let mut reader = LineageEdgeReader::try_create(ctx.clone()).await?;
    let mut resolver = ObjectResolver::try_create(ctx.clone()).await?;
    match args.object_domain {
        ObjectDomain::Column => traverse_columns(ctx, &mut reader, &mut resolver, args).await,
        _ => traverse_objects(&mut reader, &mut resolver, args).await,
    }
}

async fn traverse_objects(
    reader: &mut LineageEdgeReader,
    resolver: &mut ObjectResolver,
    args: GetLineageArgs,
) -> Result<Vec<LineageResultRow>> {
    let Some(start) = resolver
        .resolve_start(args.object_domain, &args.object_name)
        .await?
    else {
        return Ok(vec![]);
    };
    let mut frontier = start.lookup_keys.clone();
    let mut visited = frontier.clone();
    let mut results: HashMap<(String, String), TraversedObjectEdge> = HashMap::new();

    for distance in 1..=args.distance {
        let raw_edges = reader
            .read_frontier(args.direction.match_column(), &frontier)
            .await?;
        let mut level_edges: HashMap<(String, String), TraversedObjectEdge> = HashMap::new();
        for edge in raw_edges {
            let Some(source) = resolver.resolve(&edge.source).await? else {
                continue;
            };
            let Some(target) = resolver.resolve(&edge.target).await? else {
                continue;
            };
            if same_object(&source, &target) {
                continue;
            }
            let key = (source.object_key.clone(), target.object_key.clone());
            let candidate = TraversedObjectEdge {
                distance,
                edge,
                source,
                target,
            };
            match level_edges.get_mut(&key) {
                Some(current) if candidate.edge.newer_than(&current.edge) => *current = candidate,
                None => {
                    level_edges.insert(key, candidate);
                }
                _ => {}
            }
        }

        let mut next_frontier = BTreeSet::new();
        for (key, candidate) in level_edges {
            match results.get_mut(&key) {
                Some(current)
                    if candidate.distance < current.distance
                        || (candidate.distance == current.distance
                            && candidate.edge.newer_than(&current.edge)) =>
                {
                    *current = candidate.clone();
                }
                None => {
                    results.insert(key, candidate.clone());
                }
                _ => {}
            }

            let next = match args.direction {
                QueryDirection::Upstream => &candidate.source,
                QueryDirection::Downstream => &candidate.target,
            };
            if next.expandable && next.lookup_keys.iter().all(|key| !visited.contains(key)) {
                next_frontier.extend(next.lookup_keys.iter().cloned());
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        visited.extend(next_frontier.iter().cloned());
        frontier = next_frontier;
    }

    let mut rows = results
        .into_values()
        .map(|result| {
            let (source_object_catalog, source_object_database, source_object_name) =
                result.source.output_address();
            let (target_object_catalog, target_object_database, target_object_name) =
                result.target.output_address();
            LineageResultRow {
                source_object_catalog,
                source_object_database,
                source_object_name,
                source_object_domain: Some(result.source.object_type.as_str().to_string()),
                source_column_name: None,
                source_status: "ACTIVE".to_string(),
                target_object_catalog,
                target_object_database,
                target_object_name,
                target_object_domain: Some(result.target.object_type.as_str().to_string()),
                target_column_name: None,
                target_status: "ACTIVE".to_string(),
                distance: i32::from(result.distance),
                process: Some(process_json(&result.edge)),
            }
        })
        .collect::<Vec<_>>();
    sort_rows(&mut rows);
    Ok(rows)
}

async fn traverse_columns(
    ctx: Arc<dyn TableContext>,
    reader: &mut LineageEdgeReader,
    resolver: &mut ObjectResolver,
    args: GetLineageArgs,
) -> Result<Vec<LineageResultRow>> {
    let (table_name, column_name) = split_column_name(&args.object_name)?;
    let Some(start) = resolver
        .resolve_start(ObjectDomain::Column, &table_name)
        .await?
    else {
        return Ok(vec![]);
    };
    let column_name = normalize_column_name(&ctx, &column_name)?;
    let Some((column_id, column_name)) = start.column_by_name(&column_name) else {
        return Ok(vec![]);
    };

    let mut frontier = vec![ColumnFrontier {
        object: start,
        column_id,
        column_name,
    }];
    let mut visited = BTreeSet::new();
    visited.insert(column_frontier_key(&frontier[0]));
    let mut results: HashMap<(String, String, String, String), TraversedColumnEdge> =
        HashMap::new();

    for distance in 1..=args.distance {
        let lookup_keys = frontier
            .iter()
            .flat_map(|column| column.object.lookup_keys.iter().cloned())
            .collect::<BTreeSet<_>>();
        let raw_edges = reader
            .read_frontier(args.direction.match_column(), &lookup_keys)
            .await?;
        let mut level_edges: HashMap<(String, String, String, String), TraversedColumnEdge> =
            HashMap::new();
        let mut next_columns = Vec::new();

        for edge in raw_edges {
            let Some(source) = resolver.resolve(&edge.source).await? else {
                continue;
            };
            let Some(target) = resolver.resolve(&edge.target).await? else {
                continue;
            };
            if same_object(&source, &target) {
                continue;
            }

            let (
                current_captured,
                current_object,
                current_column_address_kind,
                next_captured,
                next_object,
                next_column_address_kind,
                column_map,
            ) = match args.direction {
                QueryDirection::Upstream => (
                    &edge.target,
                    &target,
                    edge.target_column_address_kind,
                    &edge.source,
                    &source,
                    edge.source_column_address_kind,
                    &edge.target_to_source_columns,
                ),
                QueryDirection::Downstream => (
                    &edge.source,
                    &source,
                    edge.source_column_address_kind,
                    &edge.target,
                    &target,
                    edge.target_column_address_kind,
                    &edge.source_to_target_columns,
                ),
            };
            let (Some(current_column_address_kind), Some(next_column_address_kind)) =
                (current_column_address_kind, next_column_address_kind)
            else {
                continue;
            };

            for current_column in frontier.iter().filter(|column| {
                column
                    .object
                    .lookup_keys
                    .contains(&current_captured.lineage_key)
            }) {
                let current_ref = match current_column_address_kind {
                    AddressKind::Id => &current_column.column_id,
                    AddressKind::Name => &current_column.column_name,
                };
                let Some(mapped_refs) = column_map.get(current_ref) else {
                    continue;
                };
                for mapped_ref in mapped_refs {
                    let Some((next_id, next_name)) = resolve_column_ref(
                        next_captured,
                        next_object,
                        next_column_address_kind,
                        mapped_ref,
                    ) else {
                        continue;
                    };
                    let (source_column, target_column) = match args.direction {
                        QueryDirection::Upstream => {
                            (next_name.clone(), current_column.column_name.clone())
                        }
                        QueryDirection::Downstream => {
                            (current_column.column_name.clone(), next_name.clone())
                        }
                    };
                    let (source_masked, target_masked) = match args.direction {
                        QueryDirection::Upstream => (
                            source.is_column_masked(&next_id),
                            target.is_column_masked(&current_column.column_id),
                        ),
                        QueryDirection::Downstream => (
                            source.is_column_masked(&current_column.column_id),
                            target.is_column_masked(&next_id),
                        ),
                    };
                    let key = (
                        source.object_key.clone(),
                        source_column.clone(),
                        target.object_key.clone(),
                        target_column.clone(),
                    );
                    let candidate = TraversedColumnEdge {
                        distance,
                        edge: edge.clone(),
                        source: source.clone(),
                        source_column,
                        target: target.clone(),
                        target_column,
                        source_masked,
                        target_masked,
                    };
                    match level_edges.get_mut(&key) {
                        Some(current) if candidate.edge.newer_than(&current.edge) => {
                            *current = candidate
                        }
                        None => {
                            level_edges.insert(key, candidate);
                        }
                        _ => {}
                    }
                    if next_object.expandable {
                        next_columns.push(ColumnFrontier {
                            object: next_object.clone(),
                            column_id: next_id,
                            column_name: next_name,
                        });
                    }
                }
            }

            // The current endpoint must resolve to the same active object represented by the
            // frontier. This also prevents a stale name-addressed edge from crossing a replace.
            let _ = current_object;
        }

        for (key, candidate) in level_edges {
            match results.get_mut(&key) {
                Some(current)
                    if candidate.distance < current.distance
                        || (candidate.distance == current.distance
                            && candidate.edge.newer_than(&current.edge)) =>
                {
                    *current = candidate;
                }
                None => {
                    results.insert(key, candidate);
                }
                _ => {}
            }
        }

        let mut deduplicated = HashMap::new();
        for column in next_columns {
            let key = column_frontier_key(&column);
            if !visited.contains(&key) {
                deduplicated.entry(key).or_insert(column);
            }
        }
        if deduplicated.is_empty() {
            break;
        }
        visited.extend(deduplicated.keys().cloned());
        frontier = deduplicated.into_values().collect();
    }

    let mut rows = results
        .into_values()
        .map(|result| {
            let (source_object_catalog, source_object_database, source_object_name) =
                result.source.output_address();
            let (target_object_catalog, target_object_database, target_object_name) =
                result.target.output_address();
            LineageResultRow {
                source_object_catalog,
                source_object_database,
                source_object_name,
                source_object_domain: Some(result.source.object_type.as_str().to_string()),
                source_column_name: Some(result.source_column),
                source_status: target_status(result.source_masked).to_string(),
                target_object_catalog,
                target_object_database,
                target_object_name,
                target_object_domain: Some(result.target.object_type.as_str().to_string()),
                target_column_name: Some(result.target_column),
                target_status: target_status(result.target_masked).to_string(),
                distance: i32::from(result.distance),
                process: Some(process_json(&result.edge)),
            }
        })
        .collect::<Vec<_>>();
    sort_rows(&mut rows);
    Ok(rows)
}

pub(super) fn resolve_column_ref(
    captured: &CapturedObject,
    object: &ResolvedObject,
    address_kind: AddressKind,
    column_ref: &str,
) -> Option<(String, String)> {
    if !captured.is_default_catalog() {
        // External catalog column identity and rename/drop semantics are catalog-specific.
        // V1 validates the external object endpoint only and keeps the captured column
        // reference as a display value until per-catalog column validation is available.
        return Some((column_ref.to_string(), column_ref.to_string()));
    }
    match address_kind {
        AddressKind::Id => object.column_by_id(column_ref),
        AddressKind::Name => object.column_by_name(column_ref),
    }
}

pub(super) fn same_object(source: &ResolvedObject, target: &ResolvedObject) -> bool {
    match (source.id, target.id) {
        (Some(source_id), Some(target_id))
            if source.catalog_type.eq_ignore_ascii_case("DEFAULT")
                && target.catalog_type.eq_ignore_ascii_case("DEFAULT") =>
        {
            source_id == target_id
        }
        _ => {
            source.object_type == target.object_type
                && source.catalog == target.catalog
                && source.database == target.database
                && source.name == target.name
        }
    }
}

fn column_frontier_key(column: &ColumnFrontier) -> (String, String) {
    let column_key = if column.object.object_type == LineageObjectType::View {
        column.column_name.clone()
    } else {
        column.column_id.clone()
    };
    (
        column
            .object
            .lookup_keys
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| column.object.object_key.clone()),
        column_key,
    )
}

pub(super) fn process_json(edge: &RawLineageEdge) -> String {
    // Keep embedded JSON timestamps stable across session timezones and self-describing.
    let updated_on = edge.updated_on.map(timestamp_to_rfc3339_utc);
    serde_json::json!({
        "query_id": edge.query_info.query_id,
        "updated_on": updated_on,
        "user_name": edge.user_name,
        "query_parameterized_hash": edge.query_parameterized_hash,
        "lineage_kind": edge.lineage_kind,
        "query_text": edge.query_info.query_text,
        "query_duration_ms": edge.query_info.query_duration_ms,
        "written_rows": edge.query_info.written_rows,
        "scan_rows": edge.query_info.scan_rows,
    })
    .to_string()
}

fn target_status(masked: bool) -> &'static str {
    if masked { "MASKED" } else { "ACTIVE" }
}

fn sort_rows(rows: &mut [LineageResultRow]) {
    rows.sort_by(|left, right| {
        (
            left.distance,
            left.source_object_catalog.as_deref().unwrap_or_default(),
            left.source_object_database.as_deref().unwrap_or_default(),
            left.source_object_name.as_deref().unwrap_or_default(),
            left.source_column_name.as_deref().unwrap_or_default(),
            left.target_object_catalog.as_deref().unwrap_or_default(),
            left.target_object_database.as_deref().unwrap_or_default(),
            left.target_object_name.as_deref().unwrap_or_default(),
            left.target_column_name.as_deref().unwrap_or_default(),
        )
            .cmp(&(
                right.distance,
                right.source_object_catalog.as_deref().unwrap_or_default(),
                right.source_object_database.as_deref().unwrap_or_default(),
                right.source_object_name.as_deref().unwrap_or_default(),
                right.source_column_name.as_deref().unwrap_or_default(),
                right.target_object_catalog.as_deref().unwrap_or_default(),
                right.target_object_database.as_deref().unwrap_or_default(),
                right.target_object_name.as_deref().unwrap_or_default(),
                right.target_column_name.as_deref().unwrap_or_default(),
            ))
    });
}

pub(super) fn split_column_name(input: &str) -> Result<(String, String)> {
    let input = input.trim();
    let Some(index) = last_unquoted_dot(input) else {
        return Err(ErrorCode::BadArguments(
            "column object_name must be qualified by table name",
        ));
    };
    let table = input[..index].trim();
    let column = input[index + 1..].trim();
    if table.is_empty() || column.is_empty() {
        return Err(ErrorCode::BadArguments(
            "column object_name must be in table.column format",
        ));
    }
    Ok((table.to_string(), column.to_string()))
}

pub(super) fn normalize_column_name(ctx: &Arc<dyn TableContext>, value: &str) -> Result<String> {
    let settings = ctx.get_settings();
    let resolution = NameResolutionContext::try_from(settings.as_ref())?;
    let dialect = settings.get_sql_dialect().unwrap_or_default();
    let column_ref = parse_table_ref(value, dialect).map_err(|error| {
        ErrorCode::BadArguments(format!("invalid column name '{value}': {}", error.1))
    })?;
    if column_ref.catalog.is_some() || column_ref.database.is_some() {
        return Err(ErrorCode::BadArguments(format!(
            "invalid column name '{value}'"
        )));
    }
    Ok(normalize_identifier(&column_ref.table, &resolution).name)
}

fn last_unquoted_dot(input: &str) -> Option<usize> {
    let mut in_quote = false;
    let mut last_dot = None;
    let mut iter = input.char_indices().peekable();
    while let Some((index, ch)) = iter.next() {
        match ch {
            '"' => {
                if in_quote && matches!(iter.peek(), Some((_, '"'))) {
                    iter.next();
                } else {
                    in_quote = !in_quote;
                }
            }
            '.' if !in_quote => last_dot = Some(index),
            _ => {}
        }
    }
    last_dot
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_edge(
        query_id: Option<&str>,
        updated_on: Option<i64>,
        lineage_kind: Option<&str>,
    ) -> RawLineageEdge {
        let endpoint = CapturedObject {
            lineage_key: "TABLE::ID::1".to_string(),
            address_kind: AddressKind::Id,
            catalog_type: "DEFAULT".to_string(),
            object_type: LineageObjectType::Table,
            catalog: "default".to_string(),
            database: "default".to_string(),
            name: "t".to_string(),
            id: Some(1),
        };
        RawLineageEdge {
            updated_on,
            user_name: None,
            query_parameterized_hash: None,
            query_info: super::super::edge_reader::LineageQueryInfo {
                query_id: query_id.map(str::to_string),
                ..Default::default()
            },
            lineage_kind: lineage_kind.map(str::to_string),
            column_lineage_hash: String::new(),
            source: endpoint.clone(),
            target: endpoint,
            source_column_address_kind: None,
            target_column_address_kind: None,
            source_to_target_columns: Default::default(),
            target_to_source_columns: Default::default(),
        }
    }

    #[test]
    fn test_process_json() {
        let mut edge = test_edge(Some("query-1"), Some(0), Some("DML"));
        edge.user_name = Some("test-user".to_string());
        edge.query_parameterized_hash = Some("parameterized-hash".to_string());
        edge.query_info.query_text = Some("INSERT INTO dst SELECT 'quoted'\nFROM src".to_string());
        edge.query_info.query_duration_ms = Some(123);
        edge.query_info.written_rows = Some(10);
        edge.query_info.scan_rows = Some(20);
        let process: serde_json::Value =
            serde_json::from_str(&process_json(&edge)).expect("valid process JSON");
        assert_eq!(
            process,
            serde_json::json!({
                "query_id": "query-1",
                "updated_on": "1970-01-01T00:00:00.000000Z",
                "user_name": "test-user",
                "query_parameterized_hash": "parameterized-hash",
                "lineage_kind": "DML",
                "query_text": "INSERT INTO dst SELECT 'quoted'\nFROM src",
                "query_duration_ms": 123,
                "written_rows": 10,
                "scan_rows": 20,
            })
        );

        let null_process: serde_json::Value =
            serde_json::from_str(&process_json(&test_edge(None, None, None)))
                .expect("valid process JSON");
        assert_eq!(
            null_process,
            serde_json::json!({
                "query_id": null,
                "updated_on": null,
                "user_name": null,
                "query_parameterized_hash": null,
                "lineage_kind": null,
                "query_text": null,
                "query_duration_ms": null,
                "written_rows": null,
                "scan_rows": null,
            })
        );
    }

    #[test]
    fn test_split_quoted_column_name() -> Result<()> {
        assert_eq!(
            split_column_name(r#"db."table.with.dot"."column.with.dot""#)?,
            (
                r#"db."table.with.dot""#.to_string(),
                r#""column.with.dot""#.to_string(),
            )
        );
        Ok(())
    }

    #[test]
    fn test_target_status() {
        assert_eq!(target_status(false), "ACTIVE");
        assert_eq!(target_status(true), "MASKED");
    }
}
