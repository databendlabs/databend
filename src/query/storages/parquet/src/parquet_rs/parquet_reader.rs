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

use std::sync::Arc;

use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_schema::ArrowError;
use arrow_schema::FieldRef;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use futures::StreamExt;
use opendal::Operator;
use parquet::arrow::arrow_reader::ArrowPredicateFn;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::arrow::arrow_to_parquet_schema;
use parquet::arrow::parquet_to_arrow_schema_by_columns;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;

use super::pruning::ParquetPruner;

struct ParquetPredicate {
    /// Columns used for eval predicate.
    pub projection: ProjectionMask,
    /// Predicate filter expression.
    pub filter: Expr,
    field_paths: Vec<(FieldRef, Vec<FieldIndex>)>,
}

pub struct ParquetRSReader {
    op: Operator,
    predicate: Option<Arc<ParquetPredicate>>,
    /// Columns to output.
    projection: ProjectionMask,
    /// If we use [`ProjectionMask`] to get inner columns of a struct,
    /// the columns will be contains in a struct array in the read [`RecordBatch`].
    ///
    /// Therefore, if `is_inner_project`, we should extract inner columns from the struct manually by traversing the nested column.
    ///
    /// If `is_inner_project` is false, we can skip the traversing.
    is_inner_project: bool,
    /// Field paths helping to traverse columns.
    field_paths: Vec<(FieldRef, Vec<FieldIndex>)>,

    pruner: ParquetPruner,
}

impl ParquetRSReader {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        table_schema: TableSchemaRef,
        arrow_schema: &arrow_schema::Schema,
        plan: &DataSourcePlan,
        options: ParquetReadOptions,
    ) -> Result<Self> {
        let mut output_projection =
            PushDownInfo::projection_of_push_downs(&table_schema, &plan.push_downs);
        let schema_descr = arrow_to_parquet_schema(arrow_schema)?;
        // Build predicate for lazy materialize (prewhere).
        let predicate = PushDownInfo::prewhere_of_push_downs(&plan.push_downs)
            .map(|prewhere| {
                output_projection = prewhere.output_columns.clone();
                let schema = prewhere.prewhere_columns.project_schema(&table_schema);
                let filter = prewhere
                    .filter
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .project_column_ref(|name| schema.index_of(name).unwrap());
                let projection = prewhere.prewhere_columns.to_arrow_projection(&schema_descr);
                let schema = to_arrow_schema(&schema);
                let batch_schema =
                    parquet_to_arrow_schema_by_columns(&schema_descr, projection.clone(), None)?;
                let field_paths = compute_output_field_paths(&schema, &batch_schema)?;
                Ok::<_, ErrorCode>(Arc::new(ParquetPredicate {
                    projection,
                    filter,
                    field_paths,
                }))
            })
            .transpose()?;
        // Build projection mask and field paths for transforming `RecordBatch` to output block.
        let projection = output_projection.to_arrow_projection(&schema_descr);
        let batch_schema =
            parquet_to_arrow_schema_by_columns(&schema_descr, projection.clone(), None)?;
        let output_schema = to_arrow_schema(&output_projection.project_schema(&table_schema));
        let field_paths = compute_output_field_paths(&output_schema, &batch_schema)?;
        // Build pruner to prune row groups and pages(TODO).
        let pruner = ParquetPruner::try_create(ctx, table_schema, &plan.push_downs, options)?;

        Ok(Self {
            op,
            predicate,
            projection,
            is_inner_project: matches!(output_projection, Projection::InnerColumns(_)),
            field_paths,
            pruner,
        })
    }

    /// Read a [`DataBlock`] from parquet file using native apache arrow-rs APIs.
    pub async fn read_block(&self, ctx: Arc<dyn TableContext>, loc: &str) -> Result<DataBlock> {
        let reader = self.op.reader(loc).await?;
        // TODO(parquet):
        // - set batch size to generate block one by one.
        // - set row selections.
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await?
            .with_projection(self.projection.clone());

        // Prune row groups.
        let file_meta = builder.metadata();
        let selected_row_groups = self.pruner.prune_row_groups(file_meta)?;
        builder = builder.with_row_groups(selected_row_groups);

        if let Some(predicate) = self.predicate.as_ref() {
            let func_ctx = ctx.get_function_context()?;
            let is_inner_project = self.is_inner_project;
            let projection = predicate.projection.clone();
            let predicate = predicate.clone();
            let predicate_fn = move |batch| {
                let ParquetPredicate {
                    filter,
                    field_paths,
                    ..
                } = predicate.as_ref();
                let res: Result<BooleanArray> = try {
                    let block = if is_inner_project {
                        transform_record_batch(&batch, field_paths)?
                    } else {
                        let (block, _) = DataBlock::from_record_batch(&batch)?;
                        block
                    };
                    let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
                    let res = evaluator
                        .run(filter)?
                        .convert_to_full_column(&DataType::Boolean, batch.num_rows())
                        .into_arrow_rs()?;
                    BooleanArray::from(res.to_data())
                };
                res.map_err(|e| ArrowError::from_external_error(Box::new(e)))
            };
            builder = builder.with_row_filter(RowFilter::new(vec![Box::new(
                ArrowPredicateFn::new(projection, predicate_fn),
            )]));
        }

        let stream = builder.build()?;
        let record_batches = stream.collect::<Vec<_>>().await;
        let blocks = if self.is_inner_project {
            record_batches
                .into_iter()
                .map(|b| {
                    let b = b?;
                    transform_record_batch(&b, &self.field_paths)
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            record_batches
                .into_iter()
                .map(|b| {
                    let (block, _) = DataBlock::from_record_batch(&b?)?;
                    Ok(block)
                })
                .collect::<Result<Vec<_>>>()?
        };

        DataBlock::concat(&blocks)
    }
}

fn to_arrow_schema(schema: &TableSchema) -> arrow_schema::Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| arrow_schema::Field::from(common_arrow::arrow::datatypes::Field::from(f)))
        .collect::<Vec<_>>();
    arrow_schema::Schema::new(fields)
}

/// Search `batch_schema` by column names from `output_schema` to compute path indices for getting columns from [`RecordBatch`].
fn compute_output_field_paths(
    output_schema: &arrow_schema::Schema,
    batch_schema: &arrow_schema::Schema,
) -> Result<Vec<(arrow_schema::FieldRef, Vec<FieldIndex>)>> {
    let output_fields = output_schema.fields();
    let parquet_schema_desc = arrow_to_parquet_schema(batch_schema)?;
    let parquet_schema = parquet_schema_desc.root_schema();

    let mut path_indices = Vec::with_capacity(output_fields.len());
    for field in output_fields {
        let name_path = field.name().split(':').collect::<Vec<_>>();
        assert!(!name_path.is_empty());
        let mut path = Vec::with_capacity(name_path.len());
        let mut ty = parquet_schema;
        for name in name_path {
            match ty {
                parquet::schema::types::Type::GroupType { fields, .. } => {
                    let idx = fields
                        .iter()
                        .position(|t| t.name().eq_ignore_ascii_case(name))
                        .ok_or(error_cannot_find_field(field.name(), parquet_schema))?;
                    path.push(idx);
                    ty = &fields[idx];
                }
                _ => return Err(error_cannot_find_field(field.name(), parquet_schema)),
            }
        }

        path_indices.push((field.clone(), path));
    }

    Ok(path_indices)
}

fn error_cannot_find_field(name: &str, schema: &parquet::schema::types::Type) -> ErrorCode {
    ErrorCode::TableSchemaMismatch(format!(
        "Cannot find field {} in the parquet schema {:?}",
        name, schema
    ))
}

/// Traverse `batch` by `path_indices` to get output [`Column`].
fn traverse_column(
    field: &arrow_schema::FieldRef,
    path: &[FieldIndex],
    batch: &RecordBatch,
) -> Result<Column> {
    assert!(!path.is_empty());
    let mut columns = batch.columns();
    let schema = batch.schema();
    for idx in path.iter().take(path.len() - 1) {
        let struct_array = columns
            .get(*idx)
            .ok_or(error_cannot_traverse_path(path, &schema))?
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or(error_cannot_traverse_path(path, &schema))?;
        columns = struct_array.columns();
    }
    let idx = *path.last().unwrap();
    let array = columns
        .get(idx)
        .ok_or(error_cannot_traverse_path(path, &schema))?;
    Ok(Column::from_arrow_rs(array.clone(), field)?)
}

fn error_cannot_traverse_path(path: &[FieldIndex], schema: &arrow_schema::Schema) -> ErrorCode {
    ErrorCode::TableSchemaMismatch(format!(
        "Cannot traverse path {:?} in the arrow schema {:?}",
        path, schema
    ))
}

/// Transform a [`RecordBatch`] to [`DataBlock`].
///
/// `field_paths` is used to traverse nested columns in `batch`.
fn transform_record_batch(
    batch: &RecordBatch,
    field_paths: &[(FieldRef, Vec<FieldIndex>)],
) -> Result<DataBlock> {
    let mut columns = Vec::with_capacity(field_paths.len());
    for (field, path) in field_paths.iter() {
        let col = traverse_column(field, path, batch)?;
        columns.push(col);
    }
    Ok(DataBlock::new_from_columns(columns))
}
