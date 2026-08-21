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

use databend_common_expression::ColumnId;
use databend_common_expression::VariantDataType;
use databend_common_frozen_api::FrozenAPI;
use serde::Deserialize;
use serde::Serialize;

/// One JSON path segment. `Index` is an array subscript; `Name` is an object key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VirtualPathSegment {
    Index(i32),
    Name(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, FrozenAPI)]
pub struct VirtualSegmentPath {
    /// Canonical JSON path, e.g. `user.name`, `users[0].id`, `user.'a.b'`.
    pub path: String,
    /// `None` means this path was only counted and not extracted as a direct column.
    pub column_id: Option<ColumnId>,
    pub data_types: Vec<VariantDataType>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, FrozenAPI)]
pub struct VirtualSegmentColumnPath {
    pub source_column_id: ColumnId,
    pub paths: Vec<VirtualSegmentPath>,
    /// False means an absent path may still exist in sidecar shared data.
    pub path_statistics_complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, FrozenAPI)]
pub struct VirtualSegmentSchema {
    /// Grouped by `source_column_id` and sorted. `path_index` is local to each source.
    pub column_paths: Vec<VirtualSegmentColumnPath>,
}

impl Default for VirtualSegmentSchema {
    fn default() -> Self {
        Self {
            column_paths: Vec::new(),
        }
    }
}

impl VirtualSegmentSchema {
    /// Builds a schema grouped by source. `path_index` follows first-seen order
    /// of each path under that source, matching block-local statistics.
    pub fn from_pending_paths(
        paths: impl IntoIterator<Item = (ColumnId, String, Option<(ColumnId, Vec<VariantDataType>)>)>,
        complete: bool,
    ) -> Self {
        let mut columns = Vec::<VirtualSegmentColumnPath>::new();
        let mut column_indexes = BTreeMap::<ColumnId, usize>::new();
        let mut path_indexes = BTreeMap::<(ColumnId, String), usize>::new();
        for (source_column_id, path, column) in paths {
            if path.is_empty() {
                continue;
            }
            let (column_id, data_types) = match column {
                Some((column_id, data_types)) => (Some(column_id), data_types),
                None => (None, Vec::new()),
            };
            if let Some(path_index) = path_indexes.get(&(source_column_id, path.clone())).copied() {
                let item = &mut columns[column_indexes[&source_column_id]].paths[path_index];
                if item.column_id.is_none() {
                    item.column_id = column_id;
                }
                for data_type in data_types {
                    if !item.data_types.contains(&data_type) {
                        item.data_types.push(data_type);
                    }
                }
                continue;
            }
            let column_index = if let Some(index) = column_indexes.get(&source_column_id).copied() {
                index
            } else {
                let index = columns.len();
                columns.push(VirtualSegmentColumnPath {
                    source_column_id,
                    paths: Vec::new(),
                    path_statistics_complete: complete,
                });
                column_indexes.insert(source_column_id, index);
                index
            };
            path_indexes.insert(
                (source_column_id, path.clone()),
                columns[column_index].paths.len(),
            );
            columns[column_index].paths.push(VirtualSegmentPath {
                path,
                column_id,
                data_types,
            });
        }
        Self {
            column_paths: columns,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.column_paths
            .iter()
            .all(|column| column.paths.is_empty())
    }

    pub fn path_count(&self) -> usize {
        self.column_paths
            .iter()
            .map(|column| column.paths.len())
            .sum()
    }

    pub fn is_path_statistics_complete(&self) -> bool {
        self.column_paths
            .iter()
            .all(|column| column.path_statistics_complete)
    }

    pub fn path(&self, source_column_id: ColumnId, path_index: u32) -> Option<&VirtualSegmentPath> {
        self.column_paths
            .iter()
            .find(|column| column.source_column_id == source_column_id)?
            .paths
            .get(path_index as usize)
    }

    pub fn find_path(&self, source_column_id: ColumnId, path: &str) -> Option<u32> {
        self.column_paths
            .iter()
            .find(|column| column.source_column_id == source_column_id)?
            .paths
            .iter()
            .position(|item| item.path == path)
            .map(|index| index as u32)
    }

    pub fn field_of_column_id(
        &self,
        column_id: ColumnId,
    ) -> Option<(ColumnId, &VirtualSegmentPath)> {
        for column in &self.column_paths {
            for path in &column.paths {
                if path.column_id == Some(column_id) {
                    return Some((column.source_column_id, path));
                }
            }
        }
        None
    }
}

/// Encodes path segments to a compact reversible JSONPath subset:
/// `user.name`, `users[0].id`, `[0].name`, `user.'a.b'`.
pub fn encode_virtual_path(segments: &[VirtualPathSegment]) -> String {
    let mut encoded = String::new();
    for segment in segments {
        match segment {
            VirtualPathSegment::Index(index) => {
                encoded.push('[');
                encoded.push_str(&index.to_string());
                encoded.push(']');
            }
            VirtualPathSegment::Name(name) => {
                if !encoded.is_empty() {
                    encoded.push('.');
                }
                if is_ident(name) {
                    encoded.push_str(name);
                } else {
                    encoded.push('\'');
                    for ch in name.chars() {
                        if ch == '\\' || ch == '\'' {
                            encoded.push('\\');
                        }
                        encoded.push(ch);
                    }
                    encoded.push('\'');
                }
            }
        }
    }
    encoded
}

pub fn decode_virtual_path(path: &str) -> Option<Vec<VirtualPathSegment>> {
    let bytes = path.as_bytes();
    let mut index = 0;
    let mut segments = Vec::new();
    while index < bytes.len() {
        if bytes[index] == b'[' {
            index += 1;
            let start = index;
            if index < bytes.len() && bytes[index] == b'-' {
                index += 1;
            }
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            if start == index || (bytes.get(start) == Some(&b'-') && start + 1 == index) {
                return None;
            }
            if index >= bytes.len() || bytes[index] != b']' {
                return None;
            }
            let value = std::str::from_utf8(&bytes[start..index])
                .ok()?
                .parse::<i32>()
                .ok()?;
            segments.push(VirtualPathSegment::Index(value));
            index += 1;
            continue;
        }

        if !segments.is_empty() {
            if bytes[index] != b'.' {
                return None;
            }
            index += 1;
            if index >= bytes.len() {
                return None;
            }
        }

        if bytes[index] == b'\'' {
            index += 1;
            let mut name = String::new();
            loop {
                if index >= path.len() {
                    return None;
                }
                let rest = &path[index..];
                if rest.starts_with('\\') {
                    let escaped = rest.chars().nth(1)?;
                    if escaped != '\\' && escaped != '\'' {
                        return None;
                    }
                    name.push(escaped);
                    index += 1 + escaped.len_utf8();
                    continue;
                }
                if rest.starts_with('\'') {
                    index += 1;
                    break;
                }
                let ch = rest.chars().next()?;
                name.push(ch);
                index += ch.len_utf8();
            }
            segments.push(VirtualPathSegment::Name(name));
            continue;
        }

        let rest = &path[index..];
        let mut chars = rest.chars();
        let first = chars.next()?;
        if !is_ident_start(first) {
            return None;
        }

        let mut end = index + first.len_utf8();
        for ch in chars {
            if !is_ident_continue(ch) {
                break;
            }
            end += ch.len_utf8();
        }
        segments.push(VirtualPathSegment::Name(path[index..end].to_string()));
        index = end;
    }
    (!segments.is_empty()).then_some(segments)
}

/// Old virtual-field display name: every segment is quoted, including indexes.
/// Kept only to match leftover `VirtualDataField.name` values.
pub fn legacy_virtual_field_name(segments: &[VirtualPathSegment]) -> String {
    let mut name = String::new();
    for segment in segments {
        name.push_str("['");
        match segment {
            VirtualPathSegment::Index(index) => name.push_str(&index.to_string()),
            VirtualPathSegment::Name(value) => name.push_str(value),
        }
        name.push_str("']");
    }
    name
}

/// Parquet virtual column suffix: `[0]['name']`. Indexes are unquoted.
pub fn encode_bracket_virtual_path(segments: &[VirtualPathSegment]) -> String {
    let mut name = String::new();
    for segment in segments {
        name.push('[');
        match segment {
            VirtualPathSegment::Index(index) => name.push_str(&index.to_string()),
            VirtualPathSegment::Name(value) => {
                name.push('\'');
                name.push_str(value);
                name.push('\'');
            }
        }
        name.push(']');
    }
    name
}

/// Parses `[0]['name']` or the fully quoted legacy form `['0']['name']`.
pub fn decode_bracket_virtual_path(path: &str) -> Option<Vec<VirtualPathSegment>> {
    let bytes = path.as_bytes();
    let mut index = 0;
    let mut segments = Vec::new();
    while index < bytes.len() {
        if bytes[index] != b'[' {
            return None;
        }
        index += 1;
        if index >= bytes.len() {
            return None;
        }
        if bytes[index] == b'\'' {
            index += 1;
            let start = index;
            while index < bytes.len() && bytes[index] != b'\'' {
                index += 1;
            }
            if index >= bytes.len() {
                return None;
            }
            let name = std::str::from_utf8(&bytes[start..index]).ok()?.to_string();
            index += 1;
            if index >= bytes.len() || bytes[index] != b']' {
                return None;
            }
            index += 1;
            segments.push(VirtualPathSegment::Name(name));
        } else {
            let start = index;
            if bytes[index] == b'-' {
                index += 1;
            }
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            if start == index || (bytes.get(start) == Some(&b'-') && start + 1 == index) {
                return None;
            }
            if index >= bytes.len() || bytes[index] != b']' {
                return None;
            }
            let value = std::str::from_utf8(&bytes[start..index])
                .ok()?
                .parse::<i32>()
                .ok()?;
            index += 1;
            segments.push(VirtualPathSegment::Index(value));
        }
    }
    (!segments.is_empty()).then_some(segments)
}

pub fn encoded_path_from_bracket_name(name: &str) -> Option<String> {
    decode_bracket_virtual_path(name).map(|segments| encode_virtual_path(&segments))
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_alphabetic()
}

fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_alphanumeric()
}

fn is_ident(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(ch) if is_ident_start(ch) => chars.all(is_ident_continue),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(value: &str) -> VirtualPathSegment {
        VirtualPathSegment::Name(value.to_string())
    }

    fn roundtrip(segments: &[VirtualPathSegment]) {
        let encoded = encode_virtual_path(segments);
        assert_eq!(decode_virtual_path(&encoded).as_deref(), Some(segments));
    }

    #[test]
    fn encodes_ident_dot_path() {
        roundtrip(&[name("user"), name("name")]);
        assert_eq!(
            encode_virtual_path(&[name("user"), name("name")]),
            "user.name"
        );
    }

    #[test]
    fn encodes_array_index_without_dot() {
        roundtrip(&[name("users"), VirtualPathSegment::Index(0), name("id")]);
        assert_eq!(
            encode_virtual_path(&[name("users"), VirtualPathSegment::Index(0), name("id")]),
            "users[0].id"
        );
        roundtrip(&[VirtualPathSegment::Index(0), name("name")]);
        assert_eq!(
            encode_virtual_path(&[VirtualPathSegment::Index(0), name("name")]),
            "[0].name"
        );
    }

    #[test]
    fn distinguishes_index_from_numeric_key() {
        assert_eq!(
            encode_virtual_path(&[VirtualPathSegment::Index(0), name("name")]),
            "[0].name"
        );
        assert_eq!(encode_virtual_path(&[name("0"), name("name")]), "'0'.name");
        assert_ne!(
            decode_virtual_path("[0].name"),
            decode_virtual_path("'0'.name")
        );
    }

    #[test]
    fn quotes_special_keys() {
        roundtrip(&[name("user"), name("a.b"), name("c")]);
        assert_eq!(
            encode_virtual_path(&[name("user"), name("a.b"), name("c")]),
            "user.'a.b'.c"
        );
        roundtrip(&[name("user"), name("it's")]);
        assert_eq!(
            encode_virtual_path(&[name("user"), name("it's")]),
            "user.'it\\'s'"
        );
        roundtrip(&[name("")]);
        assert_eq!(encode_virtual_path(&[name("")]), "''");
        roundtrip(&[name("中文")]);
        assert_eq!(
            encode_virtual_path(&[name("中文"), name("姓名")]),
            "中文.姓名"
        );
        roundtrip(&[name("日本語"), name("名前")]);
        assert_eq!(
            encode_virtual_path(&[name("user"), name("中文")]),
            "user.中文"
        );
    }
}
