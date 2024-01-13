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
use std::collections::HashSet;

use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::parquet::metadata::ColumnDescriptor;
use databend_common_arrow::parquet::metadata::SchemaDescriptor;
use databend_common_arrow::schema_projection as ap;
use databend_common_catalog::plan::Projection;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_storage::ColumnNodes;

#[allow(clippy::type_complexity)]
pub fn project_parquet_schema(
    schema: &ArrowSchema,
    schema_descr: &SchemaDescriptor,
    projection: &Projection,
) -> Result<(
    ArrowSchema,
    ColumnNodes,
    HashMap<FieldIndex, ColumnDescriptor>,
    HashSet<FieldIndex>,
)> {
    // Full schema and column leaves.

    let column_nodes = ColumnNodes::new_from_schema(schema, None);
    // Project schema
    let projected_arrow_schema = match projection {
        Projection::Columns(indices) => ap::project(schema, indices),
        Projection::InnerColumns(path_indices) => ap::inner_project(schema, path_indices),
    };
    // Project column leaves
    let projected_column_nodes = ColumnNodes {
        column_nodes: projection
            .project_column_nodes(&column_nodes)?
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
    Ok((
        projected_arrow_schema,
        projected_column_nodes,
        projected_column_descriptors,
        columns_to_read,
    ))
}
