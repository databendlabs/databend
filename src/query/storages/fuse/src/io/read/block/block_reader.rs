// Copyright 2021 Datafuse Labs.
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
use std::sync::Arc;

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_catalog::plan::Projection;
use common_catalog::table::ColumnId;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::Scalar;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_sql::field_default_value;
use common_storage::ColumnNode;
use common_storage::ColumnNodes;
use opendal::Operator;

// TODO: make BlockReader as a trait.
#[derive(Clone)]
pub struct BlockReader {
    pub(crate) operator: Operator,
    pub(crate) projection: Projection,
    pub(crate) projected_schema: TableSchemaRef,
    pub(crate) project_indices: BTreeMap<usize, (ColumnId, Field, DataType)>,
    pub(crate) column_nodes: ColumnNodes,
    pub(crate) parquet_schema_descriptor: SchemaDescriptor,
    pub(crate) default_vals: Vec<Scalar>,
}

impl BlockReader {
    pub fn create(
        operator: Operator,
        schema: TableSchemaRef,
        projection: Projection,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Arc<BlockReader>> {
        eprintln!("self schema {:#?}", schema);
        let projected_schema = match projection {
            Projection::Columns(ref indices) => TableSchemaRef::new(schema.project(indices)),
            Projection::InnerColumns(ref path_indices) => {
                Arc::new(schema.inner_project(path_indices))
            }
        };

        let arrow_schema = schema.to_arrow();
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema)?;

        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&schema));
        for x in &column_nodes.column_nodes {
            eprintln!("field: {}, ids {:?}", x.field.name, x.leaf_column_ids);
        }

        let project_column_nodes: Vec<ColumnNode> = projection
            .project_column_nodes(&column_nodes)?
            .iter()
            .map(|c| (*c).clone())
            .collect();
        let project_indices = Self::build_projection_indices(&project_column_nodes);

        // init default_vals of schema.fields
        let mut default_vals = Vec::with_capacity(projected_schema.fields().len());
        for field in projected_schema.fields() {
            default_vals.push(field_default_value(ctx.clone(), field)?);
        }

        Ok(Arc::new(BlockReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            column_nodes,
            project_indices,
            default_vals,
        }))
    }

    pub fn support_blocking_api(&self) -> bool {
        self.operator.metadata().can_blocking()
    }

    // Build non duplicate leaf_ids to avoid repeated read column from parquet
    pub(crate) fn build_projection_indices(
        columns: &[ColumnNode],
    ) -> BTreeMap<usize, (ColumnId, Field, DataType)> {
        let mut indices = BTreeMap::new();
        for column in columns {
            for (i, index) in column.leaf_ids.iter().enumerate() {
                let f: TableField = (&column.field).into();
                let data_type: DataType = f.data_type().into();
                indices.insert(
                    *index,
                    (column.leaf_column_ids[i], column.field.clone(), data_type),
                );
            }
        }
        indices
    }

    pub fn schema(&self) -> TableSchemaRef {
        self.projected_schema.clone()
    }

    pub fn data_fields(&self) -> Vec<DataField> {
        self.schema().fields().iter().map(DataField::from).collect()
    }

    pub fn data_schema(&self) -> DataSchema {
        let fields = self.data_fields();
        DataSchema::new(fields)
    }
}
