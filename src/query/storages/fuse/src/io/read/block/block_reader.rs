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
use std::sync::Arc;

use arrow_schema::Field;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_native::read::NativeColumnsReader;
use databend_common_sql::DefaultExprBinder;
use databend_common_storage::ColumnNode;
use databend_common_storage::ColumnNodes;
use opendal::Operator;

use crate::BlockReadResult;

// TODO: make BlockReader as a trait.
#[derive(Clone)]
pub struct BlockReader {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) operator: Operator,
    pub(crate) projection: Projection,
    pub(crate) projected_schema: TableSchemaRef,
    pub(crate) project_indices: BTreeMap<FieldIndex, (ColumnId, Field, DataType)>,
    pub(crate) project_column_nodes: Vec<ColumnNode>,
    pub(crate) default_vals: Vec<Scalar>,
    pub(crate) all_field_default_vals: Vec<Scalar>,
    pub query_internal_columns: bool,
    // used for mutation to update stream columns.
    pub update_stream_columns: bool,
    pub put_cache: bool,

    pub original_schema: TableSchemaRef,
    pub native_columns_reader: NativeColumnsReader,
}

fn inner_project_field_default_values(default_vals: &[Scalar], paths: &[usize]) -> Result<Scalar> {
    if paths.is_empty() {
        return Err(ErrorCode::BadArguments(
            "path should not be empty".to_string(),
        ));
    }
    let index = paths[0];
    if paths.len() == 1 {
        return Ok(default_vals[index].clone());
    }

    match &default_vals[index] {
        Scalar::Tuple(s) => inner_project_field_default_values(s, &paths[1..]),
        // If the default value of a tuple type is Null,
        // the default value of inner fields are also Null.
        Scalar::Null => Ok(Scalar::Null),
        _ => {
            if paths.len() > 1 {
                return Err(ErrorCode::BadArguments(
                    "Unable to get field default value by paths".to_string(),
                ));
            }
            inner_project_field_default_values(&[default_vals[index].clone()], &paths[1..])
        }
    }
}

impl BlockReader {
    fn project_schema_and_default_vals(
        schema: &TableSchemaRef,
        projection: &Projection,
        all_field_default_vals: &[Scalar],
    ) -> (TableSchemaRef, Vec<Scalar>) {
        match projection {
            Projection::Columns(indices) => {
                let projected_schema = TableSchemaRef::new(schema.project(indices));
                let default_vals: Vec<Scalar> = indices
                    .iter()
                    .map(|&i| all_field_default_vals[i].clone())
                    .collect();
                (projected_schema, default_vals)
            }
            Projection::InnerColumns(path_indices) => {
                let projected_schema = TableSchemaRef::new(schema.inner_project(path_indices));
                let default_vals: Vec<Scalar> = path_indices
                    .values()
                    .map(|path| {
                        inner_project_field_default_values(all_field_default_vals, path).unwrap()
                    })
                    .collect();
                (projected_schema, default_vals)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_reader(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        original_schema: TableSchemaRef,
        all_field_default_vals: Vec<Scalar>,
        projection: Projection,
        query_internal_columns: bool,
        update_stream_columns: bool,
        put_cache: bool,
    ) -> Result<Arc<BlockReader>> {
        let arrow_schema = Arc::new(original_schema.as_ref().into());
        let (projected_schema, default_vals) = Self::project_schema_and_default_vals(
            &original_schema,
            &projection,
            &all_field_default_vals,
        );

        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&original_schema));
        let project_column_nodes: Vec<ColumnNode> = projection
            .project_column_nodes(&column_nodes)?
            .iter()
            .map(|c| (*c).clone())
            .collect();
        let project_indices = Self::build_projection_indices(&project_column_nodes);

        Ok(Arc::new(BlockReader {
            ctx,
            operator,
            projection,
            projected_schema,
            project_indices,
            project_column_nodes,
            default_vals,
            all_field_default_vals,
            query_internal_columns,
            update_stream_columns,
            put_cache,
            original_schema,
            native_columns_reader: NativeColumnsReader::new()?,
        }))
    }

    pub fn create(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        schema: TableSchemaRef,
        projection: Projection,
        query_internal_columns: bool,
        update_stream_columns: bool,
        put_cache: bool,
    ) -> Result<Arc<BlockReader>> {
        let mut all_field_default_vals = Vec::with_capacity(schema.fields().len());
        let mut default_exprs_binder = DefaultExprBinder::try_new(ctx.clone())?;
        for field in schema.fields() {
            all_field_default_vals.push(default_exprs_binder.get_scalar(field)?);
        }

        Self::build_reader(
            ctx,
            operator,
            schema,
            all_field_default_vals,
            projection,
            query_internal_columns,
            update_stream_columns,
            put_cache,
        )
    }

    pub fn change_projection(&self, projection: Projection) -> Result<Arc<BlockReader>> {
        Self::build_reader(
            self.ctx.clone(),
            self.operator.clone(),
            self.original_schema.clone(),
            self.all_field_default_vals.clone(),
            projection,
            self.query_internal_columns,
            self.update_stream_columns,
            self.put_cache,
        )
    }

    // Build non duplicate leaf_indices to avoid repeated read column from parquet
    pub(crate) fn build_projection_indices(
        columns: &[ColumnNode],
    ) -> BTreeMap<FieldIndex, (ColumnId, Field, DataType)> {
        let mut indices = BTreeMap::new();
        for column in columns {
            for (i, index) in column.leaf_indices.iter().enumerate() {
                let f = DataField::try_from(&column.field).unwrap();
                indices.insert(
                    *index,
                    (
                        column.leaf_column_ids[i],
                        column.field.clone(),
                        f.data_type().clone(),
                    ),
                );
            }
        }
        indices
    }

    pub fn query_internal_columns(&self) -> bool {
        self.query_internal_columns
    }

    pub fn update_stream_columns(&self) -> bool {
        self.update_stream_columns
    }

    pub fn schema(&self) -> TableSchemaRef {
        self.projected_schema.clone()
    }

    pub fn data_fields(&self) -> Vec<DataField> {
        self.schema().fields().iter().map(DataField::from).collect()
    }

    pub fn data_schema(&self) -> DataSchema {
        self.schema().into()
    }

    pub fn report_cache_metrics<'a>(
        &self,
        block_read_res: &BlockReadResult,
        ranges: impl Iterator<Item = &'a std::ops::Range<u64>>,
    ) {
        let bytes_read_from_storage: usize = ranges
            .map(|range| range.end as usize - range.start as usize)
            .sum();

        let cache_metrics = self.ctx.get_data_cache_metrics();
        let read_from_disk_cache: usize = block_read_res
            .cached_column_data
            .iter()
            .map(|(_, bytes)| bytes.size())
            .sum();

        let read_from_in_mem_cache_array: usize = block_read_res
            .cached_column_array
            .iter()
            .map(|(_, sized_array)| sized_array.1)
            .sum();

        cache_metrics.add_cache_metrics(
            bytes_read_from_storage,
            read_from_disk_cache,
            read_from_in_mem_cache_array,
        );
    }
}
