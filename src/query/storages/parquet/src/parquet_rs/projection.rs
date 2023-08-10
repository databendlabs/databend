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
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use common_catalog::plan::Projection;
use common_exception::Result;
use common_expression::FieldIndex;
use parquet::schema::types::ColumnDescPtr;
use parquet::schema::types::SchemaDescPtr;
use parquet::schema::types::SchemaDescriptor;
use parquet::schema::types::Type;
use parquet::schema::types::TypePtr;

use crate::parquet_rs::column_nodes::ColumnNodeRS;
use crate::parquet_rs::column_nodes::ColumnNodesRS;

/// Project a [`Schema`] by picking the fields at the given indices.
pub fn project_arrow_schema_normal(schema: &Schema, indices: &[usize]) -> Schema {
    let fields = indices
        .iter()
        .map(|idx| schema.fields[*idx].clone())
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata.clone())
}

/// Project a [`Schema`] with inner columns by path.
pub fn project_arrow_schema_inner(
    schema: &Schema,
    path_indices: &BTreeMap<usize, Vec<usize>>,
) -> Schema {
    let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
    let fields = paths
        .iter()
        .map(|path| traverse_paths_arrow_schema(schema.fields().deref(), path))
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata.clone())
}

fn project_arrow_schema(schema: &Schema, projection: &Projection) -> Schema {
    match projection {
        Projection::Columns(indices) => project_arrow_schema_normal(schema, indices),
        Projection::InnerColumns(path_indices) => project_arrow_schema_inner(schema, path_indices),
    }
}

fn traverse_paths_arrow_schema(fields: &[FieldRef], path: &[usize]) -> FieldRef {
    assert!(!path.is_empty());
    let field = &fields[path[0]];
    if path.len() == 1 {
        return field.clone();
    }
    if let DataType::Struct(inner_fields) = field.data_type() {
        let fields = inner_fields
            .iter()
            .map(|inner| {
                let inner_name = format!("{}:{}", field.name(), inner.name().to_lowercase());
                Arc::new(Field::new(
                    inner_name,
                    inner.data_type().clone(),
                    inner.is_nullable(),
                ))
            })
            .collect::<Vec<_>>();
        return traverse_paths_arrow_schema(&fields, &path[1..]);
    }
    unreachable!("Unable to get field paths. Fields: {:?}", fields);
}

pub fn project_column_nodes<'a>(
    column_nodes: &'a ColumnNodesRS,
    projection: &'a Projection,
) -> Result<Vec<&'a ColumnNodeRS>> {
    let column_nodes = match projection {
        Projection::Columns(indices) => indices
            .iter()
            .map(|idx| &column_nodes.column_nodes[*idx])
            .collect(),
        Projection::InnerColumns(path_indices) => {
            let paths: Vec<&Vec<usize>> = path_indices.values().collect();
            paths
                .iter()
                .map(|path| ColumnNodesRS::traverse_path(&column_nodes.column_nodes, path))
                .collect::<Result<_>>()?
        }
    };
    Ok(column_nodes)
}

fn project_parquet_schema(schema: &SchemaDescriptor, projection: &Projection) -> SchemaDescPtr {
    let fields = schema.root_schema().get_fields();
    let mut fields = match projection {
        Projection::Columns(indices) => indices
            .iter()
            .map(|idx| fields[*idx].clone())
            .collect::<Vec<_>>(),
        Projection::InnerColumns(path_indices) => {
            let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
            paths
                .iter()
                .map(|path| {
                    let field = fields[path[0]].clone();
                    let name = field.name().to_lowercase();
                    let mut names = vec![name];
                    traverse_paths_parquet_schema(&field, &path[1..], &mut names)
                })
                .collect::<Vec<_>>()
        }
    };
    let schema = Type::group_type_builder("schema")
        .with_fields(&mut fields)
        .build()
        .unwrap();
    Arc::new(SchemaDescriptor::new(Arc::new(schema)))
}

fn rename_type(field: &Type, name: &str) -> Type {
    match field {
        Type::GroupType { basic_info, fields } => {
            let mut fields = fields.clone();
            Type::group_type_builder(name)
                .with_fields(&mut fields)
                .with_logical_type(basic_info.logical_type())
                .with_repetition(basic_info.repetition())
                .build()
                .unwrap()
        }
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => Type::primitive_type_builder(name, *physical_type)
            .with_logical_type(basic_info.logical_type())
            .with_repetition(basic_info.repetition())
            .build()
            .unwrap(),
    }
}

fn traverse_paths_parquet_schema(
    field: &TypePtr,
    path: &[usize],
    names: &mut Vec<String>,
) -> TypePtr {
    if path.is_empty() {
        return if names.is_empty() {
            field.clone()
        } else {
            Arc::new(rename_type(field.as_ref(), &names.join(":")))
        };
    }
    if let Type::GroupType { fields, .. } = field.as_ref() {
        let field = &fields[path[0]];
        names.push(field.name().to_lowercase());
        return traverse_paths_parquet_schema(field, &path[1..], names);
    }
    unreachable!(
        "Unable to get path {:?} in field: {:?}, resolved {:?}",
        path, field, names
    );
}

#[allow(clippy::type_complexity)]
pub fn project_schema_all(
    schema: &Schema,
    schema_descr: &SchemaDescriptor,
    projection: &Projection,
) -> Result<(
    Schema,
    ColumnNodesRS,
    SchemaDescPtr,
    HashSet<FieldIndex>,
    HashMap<FieldIndex, ColumnDescPtr>,
)> {
    // Full schema and column leaves.
    let column_nodes = ColumnNodesRS::new_from_schema(schema, None);
    // Project schema
    let projected_arrow_schema = project_arrow_schema(schema, projection);
    // Project column leaves
    let projected_column_nodes = ColumnNodesRS {
        column_nodes: project_column_nodes(&column_nodes, projection)?
            .iter()
            .map(|&leaf| leaf.clone())
            .collect(),
    };
    let column_nodes = &projected_column_nodes.column_nodes;
    // Project column descriptors and collect columns to read
    let mut projected_column_descriptors = HashMap::with_capacity(column_nodes.len());
    let mut columns_to_read = HashSet::with_capacity(
        column_nodes
            .iter()
            .map(|leaf| leaf.leaf_indices.len())
            .sum(),
    );
    for column_node in column_nodes {
        for index in &column_node.leaf_indices {
            columns_to_read.insert(*index);
            projected_column_descriptors.insert(*index, schema_descr.columns()[*index].clone());
        }
    }
    let projected_schema_descriptor = project_parquet_schema(schema_descr, projection);
    Ok((
        projected_arrow_schema,
        projected_column_nodes,
        projected_schema_descriptor,
        columns_to_read,
        projected_column_descriptors,
    ))
}
