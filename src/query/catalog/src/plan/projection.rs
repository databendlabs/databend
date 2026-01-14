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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Formatter;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::utils::display::display_tuple_field_name;
use databend_common_storage::ColumnNode;
use databend_common_storage::ColumnNodes;
use databend_common_storage::parquet::build_parquet_schema_tree;
use databend_common_storage::parquet::traverse_parquet_schema_tree;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub enum Projection {
    /// column indices of the table
    Columns(Vec<FieldIndex>),
    /// inner column indices for tuple data type with inner columns.
    /// the key is the column_index of ColumnEntry.
    /// the value is the path indices of inner columns.
    /// the following is an example of a tuple and the corresponding path indices:
    /// a: Tuple (          path: [0]
    ///   b1: Tuple (       path: [0, 0]
    ///     c1: Int32,      path: [0, 0, 0]
    ///     c2: String,     path: [0, 0, 1]
    ///   ),
    ///   b2: Tuple (       path: [0, 1]
    ///     d1: Int32,      path: [0, 1, 0]
    ///     d2: String,     path: [0, 1, 1]
    ///     d3: String,     path: [0, 1, 2]
    ///   )
    /// )
    InnerColumns(BTreeMap<FieldIndex, Vec<FieldIndex>>),
}

impl Projection {
    /// Build a [`Projection`] from a list of column names against the given table schema.
    ///
    /// Notes:
    /// - If all names are top-level columns, this builds a `Projection::Columns` with sorted,
    ///   deduplicated indices.
    /// - If any name refers to an inner tuple column (e.g. `a:b:c`), this builds a
    ///   `Projection::InnerColumns` with path indices.
    pub fn from_column_names<S: AsRef<str>>(schema: &TableSchema, names: &[S]) -> Result<Self> {
        let mut has_inner_column = false;
        let mut indices = Vec::with_capacity(names.len());
        let mut paths = Vec::with_capacity(names.len());
        let mut seen_paths: HashSet<Vec<FieldIndex>> = HashSet::new();

        for name in names {
            let name = name.as_ref();
            match schema.index_of(name) {
                Ok(idx) => {
                    indices.push(idx);
                    let path = vec![idx];
                    if seen_paths.insert(path.clone()) {
                        paths.push(path);
                    }
                }
                Err(_) if name.contains(':') => {
                    has_inner_column = true;
                    let path = Self::path_indices_of_column_name(schema, name)?;
                    if seen_paths.insert(path.clone()) {
                        paths.push(path);
                    }
                }
                Err(err) => return Err(err),
            }
        }

        if !has_inner_column {
            indices.sort();
            indices.dedup();
            return Ok(Projection::Columns(indices));
        }

        let path_indices = paths.into_iter().enumerate().collect::<BTreeMap<_, _>>();
        Ok(Projection::InnerColumns(path_indices))
    }

    /// Merge `other` into `self` (set union).
    ///
    /// - `Columns + Columns` results in sorted, deduplicated column indices.
    /// - If either side is `InnerColumns`, the result is `InnerColumns` and preserves inner paths.
    pub fn merge(&mut self, other: &Projection) {
        fn merge_inner_paths(
            dst: &mut BTreeMap<FieldIndex, Vec<FieldIndex>>,
            src_paths: impl Iterator<Item = Vec<FieldIndex>>,
        ) {
            let mut seen: HashSet<Vec<FieldIndex>> = dst.values().cloned().collect();
            let mut next_key = dst.keys().next_back().map(|k| k + 1).unwrap_or(0);

            for path in src_paths {
                if !seen.insert(path.clone()) {
                    continue;
                }
                dst.insert(next_key, path);
                next_key += 1;
            }
        }

        match self {
            Projection::Columns(dst) => match other {
                Projection::Columns(src) => {
                    dst.extend_from_slice(src);
                    dst.sort_unstable();
                    dst.dedup();
                }
                Projection::InnerColumns(src) => {
                    let mut cols = std::mem::take(dst);
                    cols.sort_unstable();
                    cols.dedup();

                    let mut merged = BTreeMap::new();
                    merge_inner_paths(&mut merged, cols.into_iter().map(|idx| vec![idx]));
                    merge_inner_paths(&mut merged, src.values().cloned());

                    *self = Projection::InnerColumns(merged);
                }
            },
            Projection::InnerColumns(dst) => match other {
                Projection::InnerColumns(src) => {
                    merge_inner_paths(dst, src.values().cloned());
                }
                Projection::Columns(src) => {
                    let mut cols = src.clone();
                    cols.sort_unstable();
                    cols.dedup();
                    merge_inner_paths(dst, cols.into_iter().map(|idx| vec![idx]));
                }
            },
        }
    }

    /// Return a projection that removes columns covered by `other`.
    ///
    /// - A root column in `other` (e.g. `[0]`) covers the whole column and any of its inner paths.
    /// - An inner path in `other` only covers the exact path.
    pub fn difference(&self, other: &Projection) -> Projection {
        let (covered_roots, other_paths) = match other {
            Projection::Columns(indices) => (
                indices.iter().copied().collect::<HashSet<_>>(),
                HashSet::new(),
            ),
            Projection::InnerColumns(path_indices) => {
                let mut covered_roots = HashSet::new();
                let mut other_paths = HashSet::new();
                for path in path_indices.values() {
                    if path.len() == 1 {
                        covered_roots.insert(path[0]);
                    }
                    other_paths.insert(path.clone());
                }
                (covered_roots, other_paths)
            }
        };

        let mut kept_paths = Vec::new();
        match self {
            Projection::Columns(indices) => {
                for idx in indices {
                    if covered_roots.contains(idx) {
                        continue;
                    }
                    kept_paths.push(vec![*idx]);
                }
            }
            Projection::InnerColumns(path_indices) => {
                for path in path_indices.values() {
                    if covered_roots.contains(&path[0]) {
                        continue;
                    }
                    if other_paths.contains(path) {
                        continue;
                    }
                    kept_paths.push(path.clone());
                }
            }
        }

        if kept_paths.iter().all(|p| p.len() == 1) {
            let mut cols: Vec<FieldIndex> = kept_paths.into_iter().map(|p| p[0]).collect();
            cols.sort_unstable();
            cols.dedup();
            Projection::Columns(cols)
        } else {
            let mut unique_paths = HashSet::new();
            let mut path_indices = BTreeMap::new();
            let mut next_key = 0;
            for path in kept_paths {
                if unique_paths.insert(path.clone()) {
                    path_indices.insert(next_key, path);
                    next_key += 1;
                }
            }
            Projection::InnerColumns(path_indices)
        }
    }

    fn path_indices_of_column_name(schema: &TableSchema, name: &str) -> Result<Vec<FieldIndex>> {
        let (first, rest) = name.split_once(':').ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "Invalid inner column name \"{}\"; expected \":\" separated path",
                name
            ))
        })?;

        let (top_index, top_field) = schema.column_with_name(first).ok_or_else(|| {
            let valid_fields: Vec<String> =
                schema.fields().iter().map(|f| f.name.clone()).collect();
            ErrorCode::BadArguments(format!(
                "Unable to get field named \"{}\". Valid fields: {:?}",
                first, valid_fields
            ))
        })?;

        let mut path = vec![top_index];
        let mut data_type = top_field.data_type.remove_nullable();

        for seg in rest.split(':') {
            let (fields_name, fields_type) = match &data_type {
                TableDataType::Tuple {
                    fields_name,
                    fields_type,
                } => (fields_name, fields_type),
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Unable to resolve inner path \"{}\" under non-tuple column \"{}\"",
                        name, first
                    )));
                }
            };

            let mut selected_idx: Option<usize> = None;
            if let Ok(pos) = seg.parse::<usize>() {
                if pos >= 1 && pos <= fields_name.len() {
                    selected_idx = Some(pos - 1);
                }
            }
            if selected_idx.is_none() {
                selected_idx = fields_name
                    .iter()
                    .position(|n| seg == n || seg == display_tuple_field_name(n));
            }
            let selected_idx = selected_idx.ok_or_else(|| {
                ErrorCode::BadArguments(format!(
                    "Unable to resolve tuple field \"{}\" under \"{}\"",
                    seg, first
                ))
            })?;

            path.push(selected_idx);
            data_type = fields_type[selected_idx].remove_nullable();
        }

        Ok(path)
    }

    pub fn len(&self) -> usize {
        match self {
            Projection::Columns(indices) => indices.len(),
            Projection::InnerColumns(path_indices) => path_indices.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Projection::Columns(indices) => indices.is_empty(),
            Projection::InnerColumns(path_indices) => path_indices.is_empty(),
        }
    }

    /// Use this projection to project a schema.
    pub fn project_schema(&self, schema: &TableSchema) -> TableSchema {
        match self {
            Projection::Columns(indices) => schema.project(indices),
            Projection::InnerColumns(path_indices) => schema.inner_project(path_indices),
        }
    }

    pub fn project_column_nodes<'a>(
        &'a self,
        column_nodes: &'a ColumnNodes,
    ) -> Result<Vec<&'a ColumnNode>> {
        let column_nodes = match self {
            Projection::Columns(indices) => indices
                .iter()
                .map(|idx| &column_nodes.column_nodes[*idx])
                .collect(),
            Projection::InnerColumns(path_indices) => {
                let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                paths
                    .iter()
                    .map(|path| ColumnNodes::traverse_path(&column_nodes.column_nodes, path))
                    .collect::<Result<_>>()?
            }
        };
        Ok(column_nodes)
    }

    pub fn add_col(&mut self, col: FieldIndex) {
        match self {
            Projection::Columns(indices) => {
                if indices.contains(&col) {
                    return;
                }
                indices.push(col);
                indices.sort();
            }
            Projection::InnerColumns(path_indices) => {
                path_indices.entry(col).or_insert(vec![col]);
            }
        }
    }

    pub fn remove_col(&mut self, col: FieldIndex) {
        match self {
            Projection::Columns(indices) => {
                if let Some(pos) = indices.iter().position(|x| *x == col) {
                    indices.remove(pos);
                }
            }
            Projection::InnerColumns(path_indices) => {
                path_indices.remove(&col);
            }
        }
    }

    /// Convert [`Projection`] to [`ProjectionMask`] and the underlying leaf indices.
    pub fn to_arrow_projection(
        &self,
        schema: &SchemaDescriptor,
    ) -> (ProjectionMask, Vec<FieldIndex>) {
        match self {
            Projection::Columns(indices) => {
                let mask = ProjectionMask::roots(schema, indices.clone());
                let leaves = (0..schema.num_columns())
                    .filter(|i| mask.leaf_included(*i))
                    .collect();
                (mask, leaves)
            }
            Projection::InnerColumns(path_indices) => {
                let mut leave_id = 0;
                let tree = build_parquet_schema_tree(schema.root_schema(), &mut leave_id);
                assert_eq!(leave_id, schema.num_columns());
                let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                let mut leaves = vec![];
                for path in paths {
                    traverse_parquet_schema_tree(&tree, path, &mut leaves);
                }
                (ProjectionMask::leaves(schema, leaves.clone()), leaves)
            }
        }
    }
}

impl core::fmt::Debug for Projection {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Projection::Columns(indices) => write!(f, "{:?}", indices),
            Projection::InnerColumns(path_indices) => {
                let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                write!(f, "{:?}", paths)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::btreemap;

    use super::Projection;

    #[test]
    fn test_projection_difference_columns() {
        let lhs = Projection::Columns(vec![0, 1, 2, 3]);
        let rhs = Projection::Columns(vec![1, 3]);
        assert_eq!(lhs.difference(&rhs), Projection::Columns(vec![0, 2]));
    }

    #[test]
    fn test_projection_difference_inner_columns() {
        let lhs = Projection::InnerColumns(btreemap! {0 => vec![0, 0], 1 => vec![1, 0]});
        let rhs = Projection::InnerColumns(btreemap! {0 => vec![0, 0]});
        assert_eq!(
            lhs.difference(&rhs),
            Projection::InnerColumns(btreemap! {0 => vec![1, 0]})
        );
    }

    #[test]
    fn test_projection_difference_root_covers_inner() {
        let lhs = Projection::InnerColumns(btreemap! {0 => vec![0, 0], 1 => vec![1, 0]});
        let rhs = Projection::Columns(vec![0]);
        assert_eq!(
            lhs.difference(&rhs),
            Projection::InnerColumns(btreemap! {0 => vec![1, 0]})
        );
    }

    #[test]
    fn test_projection_difference_inner_does_not_cover_root() {
        let lhs = Projection::Columns(vec![0]);
        let rhs = Projection::InnerColumns(btreemap! {0 => vec![0, 1]});
        assert_eq!(lhs.difference(&rhs), Projection::Columns(vec![0]));
    }
}
