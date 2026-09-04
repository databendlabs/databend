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

use databend_common_expression::ColumnId;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;
use jsonb::keypath::OwnedKeyPaths;

/// Query-level canonical path and its precomputed segment-lookup prefixes.
///
/// Construct this once from the complete virtual-column field list and reuse it
/// while projecting every segment touched by the query.
#[derive(Debug, Eq, PartialEq)]
pub struct ProjectedVirtualPath {
    path: String,
    object_prefix: String,
    array_prefix: String,
    /// Canonical parent prefixes ordered from the nearest parent to the root.
    parent_prefixes: Vec<String>,
}

impl ProjectedVirtualPath {
    pub fn new(key_paths: &OwnedKeyPaths) -> Self {
        let path = key_paths.to_canonical_path();
        let object_prefix = format!("{}.", path);
        let array_prefix = format!("{}[", path);
        let mut parent = key_paths.clone();
        let mut parent_prefixes = Vec::with_capacity(parent.paths.len().saturating_sub(1));
        while parent.paths.len() > 1 {
            parent.paths.pop();
            parent_prefixes.push(parent.to_canonical_path());
        }
        Self {
            path,
            object_prefix,
            array_prefix,
            parent_prefixes,
        }
    }
}

/// Query-local projection of a segment's virtual column schema.
///
/// Map keys are limited to paths requested by the query. Related paths observed
/// in the segment are retained in each field's `ancestors` and `descendants`
/// lists, preserving their segment-local column IDs. The projection identifies
/// when footer planning is required; it does not copy non-requested keys from an
/// ancestor's complete subtree into the map. Recursive footer planning walks that
/// authoritative per-block trie directly, so sibling descendants remain available
/// without expanding query-local map keys.
#[derive(Clone, Debug, Default)]
pub struct ProjectedVirtualSegmentSchema {
    /// Keyed only by source column ID and query-requested canonical path.
    pub sources: HashMap<ColumnId, HashMap<String, ProjectedVirtualSegmentField>>,
}

#[derive(Clone, Debug, Default)]
pub struct ProjectedVirtualSegmentField {
    /// Segment-local ID of the exact path. `None` when the requested path is
    /// absent but related paths may still be materialized.
    pub column_id: Option<ColumnId>,

    /// Segment-observed ancestor path candidates as `(segment-local column ID, canonical path)`,
    /// ordered from the nearest parent to the root. Block or sidecar metadata determines
    /// whether a candidate can provide a Jsonb parent when creating a query read plan.
    pub ancestors: Vec<(ColumnId, String)>,

    /// Segment-observed descendant path candidates as
    /// `(segment-local column ID, canonical path)`, ordered by canonical path.
    /// Block sidecar metadata determines which candidates can reconstruct the requested object.
    pub descendants: Vec<(ColumnId, String)>,
}

impl ProjectedVirtualSegmentField {
    /// Whether reading this field may require a sidecar-derived parent/object plan
    /// in addition to, or instead of, its exact materialized column.
    pub fn has_related_paths(&self) -> bool {
        !self.ancestors.is_empty() || !self.descendants.is_empty()
    }
}

impl ProjectedVirtualSegmentSchema {
    /// Projects one persisted segment schema using query-level paths whose
    /// canonical prefixes were already computed and deduplicated by the caller.
    /// Duplicate inputs remain harmless because the final map insertion uses the
    /// same `(source_column_id, path)` key.
    pub fn project(
        schema: &VirtualSegmentSchema,
        requested_paths: &[(ColumnId, ProjectedVirtualPath)],
    ) -> Self {
        let mut projected = Self::default();
        for (source_column_id, requested_path) in requested_paths {
            let mut requested_field = ProjectedVirtualSegmentField::default();
            if let Ok(source_index) = schema
                .column_paths
                .binary_search_by_key(source_column_id, |source| source.source_column_id)
            {
                let source = &schema.column_paths[source_index];
                // Persisted segment paths are strictly sorted by canonical path. Keep exact
                // lookup and descendant extraction logarithmic by relying on that invariant.
                if let Ok(index) = source
                    .paths
                    .binary_search_by(|path| path.path.as_str().cmp(requested_path.path.as_str()))
                {
                    requested_field.column_id = Some(source.paths[index].column_id);
                }

                let descendant_range = |prefix: &str| {
                    let start = source
                        .paths
                        .partition_point(|path| path.path.as_str() < prefix);
                    let end = start
                        + source.paths[start..]
                            .partition_point(|path| path.path.starts_with(prefix));
                    start..end
                };
                let object_range = descendant_range(requested_path.object_prefix.as_str());
                let array_range = descendant_range(requested_path.array_prefix.as_str());
                requested_field
                    .descendants
                    .reserve(object_range.len() + array_range.len());
                for range in [object_range, array_range] {
                    requested_field.descendants.extend(
                        source.paths[range]
                            .iter()
                            .map(|path| (path.column_id, path.path.clone())),
                    );
                }

                // A sidecar trie can use a segment-observed ancestor as a JSONB parent
                // when that representation exists in the current block. Parent prefixes
                // are query-local invariants precomputed by `ProjectedVirtualPath`.
                requested_field
                    .ancestors
                    .reserve(requested_path.parent_prefixes.len());
                requested_field
                    .ancestors
                    .extend(requested_path.parent_prefixes.iter().filter_map(|prefix| {
                        let index = source
                            .paths
                            .binary_search_by(|path| path.path.as_str().cmp(prefix.as_str()))
                            .ok()?;
                        let path = &source.paths[index];
                        Some((path.column_id, path.path.clone()))
                    }));
            }
            projected
                .sources
                .entry(*source_column_id)
                .or_default()
                .insert(requested_path.path.clone(), requested_field);
        }
        projected
    }

    pub fn get(
        &self,
        source_column_id: ColumnId,
        path: &str,
    ) -> Option<&ProjectedVirtualSegmentField> {
        self.sources.get(&source_column_id)?.get(path)
    }

    pub fn find_column_id(&self, source_column_id: ColumnId, path: &str) -> Option<ColumnId> {
        self.get(source_column_id, path)?.column_id
    }

    pub fn has_descendants(&self, source_column_id: ColumnId, path: &str) -> bool {
        self.get(source_column_id, path)
            .is_some_and(|field| !field.descendants.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use databend_storages_common_table_meta::meta::VirtualSegmentColumnPath;
    use databend_storages_common_table_meta::meta::VirtualSegmentPath;

    use super::*;

    fn requested_path(source_column_id: ColumnId, path: &str) -> (ColumnId, ProjectedVirtualPath) {
        let key_paths = OwnedKeyPaths::from_canonical_path(path).unwrap();
        (source_column_id, ProjectedVirtualPath::new(&key_paths))
    }

    #[test]
    fn test_projected_virtual_path() {
        let key_paths = OwnedKeyPaths::from_canonical_path("a.'b.c'[0].d").unwrap();
        let path = ProjectedVirtualPath::new(&key_paths);
        assert_eq!(path.path, "a.'b.c'[0].d");
        assert_eq!(path.object_prefix, "a.'b.c'[0].d.");
        assert_eq!(path.array_prefix, "a.'b.c'[0].d[");
        assert_eq!(path.parent_prefixes, vec!["a.'b.c'[0]", "a.'b.c'", "a"]);
    }

    #[test]
    fn test_project_root_array_and_escaped_quoted_ancestors() {
        let schema = VirtualSegmentSchema {
            column_paths: vec![VirtualSegmentColumnPath {
                source_column_id: 1,
                paths: vec![
                    VirtualSegmentPath {
                        path: "'a\\'b.c'".to_string(),
                        column_id: 10,
                    },
                    VirtualSegmentPath {
                        path: "[0]".to_string(),
                        column_id: 11,
                    },
                    VirtualSegmentPath {
                        path: "[0][1]".to_string(),
                        column_id: 12,
                    },
                ],
            }],
        };
        let requested_paths = vec![
            requested_path(1, "'a\\'b.c'.d"),
            requested_path(1, "[0][1].a"),
        ];

        let projected = ProjectedVirtualSegmentSchema::project(&schema, &requested_paths);

        assert_eq!(projected.get(1, "'a\\'b.c'.d").unwrap().ancestors, vec![(
            10,
            "'a\\'b.c'".to_string()
        )]);
        assert_eq!(projected.get(1, "[0][1].a").unwrap().ancestors, vec![
            (12, "[0][1]".to_string()),
            (11, "[0]".to_string())
        ]);
    }

    #[test]
    fn test_project_keeps_sources_isolated_and_missing_source_entries() {
        let schema = VirtualSegmentSchema {
            column_paths: vec![
                VirtualSegmentColumnPath {
                    source_column_id: 1,
                    paths: vec![VirtualSegmentPath {
                        path: "a.b".to_string(),
                        column_id: 10,
                    }],
                },
                VirtualSegmentColumnPath {
                    source_column_id: 2,
                    paths: vec![VirtualSegmentPath {
                        path: "a".to_string(),
                        column_id: 11,
                    }],
                },
            ],
        };
        let requested_paths = vec![requested_path(1, "a"), requested_path(3, "a")];

        let projected = ProjectedVirtualSegmentSchema::project(&schema, &requested_paths);

        let source_one = projected.get(1, "a").unwrap();
        assert_eq!(source_one.column_id, None);
        assert!(source_one.ancestors.is_empty());
        assert_eq!(source_one.descendants, vec![(10, "a.b".to_string())]);

        let missing_source = projected.get(3, "a").unwrap();
        assert_eq!(missing_source.column_id, None);
        assert!(!missing_source.has_related_paths());
        assert!(projected.get(2, "a").is_none());
    }

    #[test]
    fn test_project_quoted_and_array_paths() {
        let schema = VirtualSegmentSchema {
            column_paths: vec![VirtualSegmentColumnPath {
                source_column_id: 1,
                paths: vec![
                    VirtualSegmentPath {
                        path: "'a.b'".to_string(),
                        column_id: 10,
                    },
                    VirtualSegmentPath {
                        path: "'a.b'.c".to_string(),
                        column_id: 11,
                    },
                    VirtualSegmentPath {
                        path: "[0]".to_string(),
                        column_id: 12,
                    },
                    VirtualSegmentPath {
                        path: "[0].a".to_string(),
                        column_id: 13,
                    },
                    VirtualSegmentPath {
                        path: "[0][1]".to_string(),
                        column_id: 14,
                    },
                ],
            }],
        };
        let requested_paths = vec![requested_path(1, "'a.b'.c"), requested_path(1, "[0]")];

        let projected = ProjectedVirtualSegmentSchema::project(&schema, &requested_paths);

        let quoted_child = projected.get(1, "'a.b'.c").unwrap();
        assert_eq!(quoted_child.column_id, Some(11));
        assert_eq!(quoted_child.ancestors, vec![(10, "'a.b'".to_string())]);
        assert!(quoted_child.descendants.is_empty());

        let array_parent = projected.get(1, "[0]").unwrap();
        assert_eq!(array_parent.column_id, Some(12));
        assert!(array_parent.ancestors.is_empty());
        assert_eq!(array_parent.descendants, vec![
            (13, "[0].a".to_string()),
            (14, "[0][1]".to_string())
        ]);
    }

    #[test]
    fn test_project_virtual_segment_schema() {
        let schema = VirtualSegmentSchema {
            column_paths: vec![VirtualSegmentColumnPath {
                source_column_id: 1,
                paths: vec![
                    VirtualSegmentPath {
                        path: "geo".to_string(),
                        column_id: 10,
                    },
                    VirtualSegmentPath {
                        path: "geo.lat".to_string(),
                        column_id: 11,
                    },
                    VirtualSegmentPath {
                        path: "geo[0]".to_string(),
                        column_id: 12,
                    },
                    VirtualSegmentPath {
                        path: "geox".to_string(),
                        column_id: 13,
                    },
                    VirtualSegmentPath {
                        path: "other.value".to_string(),
                        column_id: 14,
                    },
                ],
            }],
        };
        let requested_paths = vec![
            requested_path(1, "geo"),
            requested_path(1, "geo.lat"),
            requested_path(1, "missing"),
            requested_path(1, "geo"),
        ];

        let projected = ProjectedVirtualSegmentSchema::project(&schema, &requested_paths);
        assert_eq!(projected.sources.get(&1).unwrap().len(), 3);

        let geo = projected.get(1, "geo").unwrap();
        assert_eq!(geo.column_id, Some(10));
        assert!(geo.ancestors.is_empty());
        assert!(geo.has_related_paths());
        assert_eq!(geo.descendants, vec![
            (11, "geo.lat".to_string()),
            (12, "geo[0]".to_string())
        ]);
        assert_eq!(projected.find_column_id(1, "geo.lat"), Some(11));
        let geo_lat = projected.get(1, "geo.lat").unwrap();
        assert_eq!(geo_lat.ancestors, vec![(10, "geo".to_string())]);
        assert!(geo_lat.descendants.is_empty());
        assert!(geo_lat.has_related_paths());
        assert!(projected.get(1, "geo[0]").is_none());
        assert!(projected.has_descendants(1, "geo"));
        assert!(!projected.has_descendants(1, "geo.lat"));
        assert!(projected.get(1, "geox").is_none());
        assert!(projected.get(1, "other.value").is_none());

        let missing = projected.get(1, "missing").unwrap();
        assert_eq!(missing.column_id, None);
        assert!(missing.ancestors.is_empty());
        assert!(missing.descendants.is_empty());
        assert!(!missing.has_related_paths());
    }

    #[test]
    fn test_project_records_ancestors_for_absent_requested_path() {
        let schema = VirtualSegmentSchema {
            column_paths: vec![VirtualSegmentColumnPath {
                source_column_id: 1,
                paths: vec![
                    VirtualSegmentPath {
                        path: "a".to_string(),
                        column_id: 20,
                    },
                    VirtualSegmentPath {
                        path: "a.b".to_string(),
                        column_id: 21,
                    },
                    VirtualSegmentPath {
                        path: "a.x".to_string(),
                        column_id: 22,
                    },
                ],
            }],
        };

        let projected =
            ProjectedVirtualSegmentSchema::project(&schema, &[requested_path(1, "a.b.c")]);
        assert_eq!(projected.sources.get(&1).unwrap().len(), 1);

        let requested = projected.get(1, "a.b.c").unwrap();
        assert_eq!(requested.column_id, None);
        assert_eq!(requested.ancestors, vec![
            (21, "a.b".to_string()),
            (20, "a".to_string())
        ]);
        assert!(requested.descendants.is_empty());
        assert!(requested.has_related_paths());
        assert_eq!(requested.ancestors.first(), Some(&(21, "a.b".to_string())));
        // Sibling paths are neither requested entries nor classified as ancestors.
        assert!(projected.get(1, "a.x").is_none());
    }

    #[test]
    fn test_project_keeps_requested_path_ancestors() {
        let schema = VirtualSegmentSchema {
            column_paths: vec![VirtualSegmentColumnPath {
                source_column_id: 1,
                paths: vec![
                    VirtualSegmentPath {
                        path: "a".to_string(),
                        column_id: 20,
                    },
                    VirtualSegmentPath {
                        path: "a.b".to_string(),
                        column_id: 21,
                    },
                    VirtualSegmentPath {
                        path: "a.b.c".to_string(),
                        column_id: 22,
                    },
                    VirtualSegmentPath {
                        path: "a.x".to_string(),
                        column_id: 23,
                    },
                ],
            }],
        };

        let projected =
            ProjectedVirtualSegmentSchema::project(&schema, &[requested_path(1, "a.b.c")]);
        assert_eq!(projected.sources.get(&1).unwrap().len(), 1);

        let requested = projected.get(1, "a.b.c").unwrap();
        assert_eq!(requested.column_id, Some(22));
        assert_eq!(requested.ancestors, vec![
            (21, "a.b".to_string()),
            (20, "a".to_string())
        ]);
        assert!(requested.descendants.is_empty());
        assert!(requested.has_related_paths());
        assert_eq!(requested.ancestors.first(), Some(&(21, "a.b".to_string())));
        assert!(projected.get(1, "a").is_none());
        assert!(projected.get(1, "a.b").is_none());
        assert_eq!(projected.find_column_id(1, "a.b.c"), Some(22));
        assert!(projected.get(1, "a.x").is_none());
        assert!(!projected.has_descendants(1, "a.b.c"));
    }
}
