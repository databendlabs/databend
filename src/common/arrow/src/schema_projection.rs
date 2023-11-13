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

use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::Field;
use crate::arrow::datatypes::Schema;

/// Project a [`Schema`] by picking the fields at the given indices.
pub fn project(schema: &Schema, indices: &[usize]) -> Schema {
    let fields = indices
        .iter()
        .map(|idx| schema.fields[*idx].clone())
        .collect::<Vec<_>>();
    Schema::with_metadata(fields.into(), schema.metadata.clone())
}

/// Project a [`Schema`] with inner columns by path.
pub fn inner_project(schema: &Schema, path_indices: &BTreeMap<usize, Vec<usize>>) -> Schema {
    let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
    let fields = paths
        .iter()
        .map(|path| traverse_paths(&schema.fields, path))
        .collect::<Vec<_>>();
    Schema::with_metadata(fields.into(), schema.metadata.clone())
}

fn traverse_paths(fields: &[Field], path: &[usize]) -> Field {
    assert!(!path.is_empty());
    let field = &fields[path[0]];
    if path.len() == 1 {
        return field.clone();
    }
    if let DataType::Struct(inner_fields) = field.data_type() {
        let fields = inner_fields
            .iter()
            .map(|inner| {
                let inner_name = format!("{}:{}", field.name, inner.name.to_lowercase());
                Field {
                    name: inner_name,
                    ..inner.clone()
                }
            })
            .collect::<Vec<_>>();
        return traverse_paths(&fields, &path[1..]);
    }
    unreachable!("Unable to get field paths. Fields: {:?}", fields);
}
