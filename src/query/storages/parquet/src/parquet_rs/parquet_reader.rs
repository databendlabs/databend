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

use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_schema::ArrowError;
use arrow_schema::FieldRef;
use bytes::Bytes;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::FieldIndex;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use futures::StreamExt;
use opendal::Operator;
use opendal::Reader;
use parquet::arrow::arrow_reader::ArrowPredicateFn;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::arrow::arrow_to_parquet_schema;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::parquet_to_arrow_schema_by_columns;
use parquet::arrow::FieldLevels;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use parquet::format::PageLocation;
use parquet::schema::types::SchemaDescriptor;

use super::predicate::ParquetPredicate;
use super::pruning::ParquetRSPruner;
use super::row_group::InMemoryRowGroup;
use crate::ParquetRSRowGroupPart;

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
    /// Projected field levels.
    field_levels: FieldLevels,

    pruner: Option<ParquetRSPruner>,

    // Options
    need_page_index: bool,
    batch_size: usize,
}

impl ParquetRSReader {
    pub fn operator(&self) -> Operator {
        self.op.clone()
    }

    pub fn create(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        table_schema: TableSchemaRef,
        arrow_schema: &arrow_schema::Schema,
        plan: &DataSourcePlan,
        options: ParquetReadOptions,
        create_pruner: bool, // If prune row groups and pages before reading.
    ) -> Result<Self> {
        let schema_desc = arrow_to_parquet_schema(arrow_schema)?;
        Self::create_with_parquet_schema(
            ctx,
            op,
            table_schema,
            &schema_desc,
            plan,
            options,
            create_pruner,
        )
    }

    pub fn create_with_parquet_schema(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        table_schema: TableSchemaRef,
        schema_desc: &SchemaDescriptor,
        plan: &DataSourcePlan,
        options: ParquetReadOptions,
        create_pruner: bool, // If prune row groups and pages before reading.
    ) -> Result<Self> {
        let mut output_projection =
            PushDownInfo::projection_of_push_downs(&table_schema, &plan.push_downs);
        // Build predicate for lazy materialize (prewhere).
        let predicate = PushDownInfo::prewhere_of_push_downs(&plan.push_downs)
            .map(|prewhere| {
                output_projection = prewhere.output_columns.clone();
                let schema = prewhere.prewhere_columns.project_schema(&table_schema);
                let filter = prewhere
                    .filter
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .project_column_ref(|name| schema.index_of(name).unwrap());
                let (projection, _) = prewhere.prewhere_columns.to_arrow_projection(schema_desc);
                let schema = to_arrow_schema(&schema);
                let batch_schema =
                    parquet_to_arrow_schema_by_columns(schema_desc, projection.clone(), None)?;
                let field_paths = compute_output_field_paths(&schema, &batch_schema)?;
                let field_levels =
                    parquet_to_arrow_field_levels(schema_desc, projection.clone(), None)?;
                Ok::<_, ErrorCode>(Arc::new(ParquetPredicate::new(
                    ctx.get_function_context()?,
                    projection,
                    field_levels,
                    filter,
                    field_paths,
                    matches!(prewhere.prewhere_columns, Projection::InnerColumns(_)),
                )))
            })
            .transpose()?;
        // Build projection mask and field paths for transforming `RecordBatch` to output block.
        // The number of columns in `output_projection` may be less than the number of actual read columns.
        let (projection, _) = output_projection.to_arrow_projection(schema_desc);
        let batch_schema =
            parquet_to_arrow_schema_by_columns(schema_desc, projection.clone(), None)?;
        let output_schema = to_arrow_schema(&output_projection.project_schema(&table_schema));
        let field_paths = compute_output_field_paths(&output_schema, &batch_schema)?;

        let pruner = if create_pruner {
            Some(ParquetRSPruner::try_create(
                ctx.get_function_context()?,
                table_schema,
                &plan.push_downs,
                options,
            )?)
        } else {
            None
        };

        let batch_size = ctx.get_settings().get_max_block_size()? as usize;
        let field_levels = parquet_to_arrow_field_levels(schema_desc, projection.clone(), None)?;

        Ok(Self {
            op,
            predicate,
            projection,
            is_inner_project: matches!(output_projection, Projection::InnerColumns(_)),
            field_paths,
            pruner,
            need_page_index: options.prune_pages(),
            batch_size,
            field_levels,
        })
    }

    pub async fn prepare_data_stream(&self, loc: &str) -> Result<ParquetRecordBatchStream<Reader>> {
        let reader: Reader = self.op.reader(loc).await?;
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(
            reader,
            ArrowReaderOptions::new()
                .with_page_index(self.need_page_index)
                .with_skip_arrow_metadata(true),
        )
        .await?
        .with_projection(self.projection.clone())
        .with_batch_size(self.batch_size);

        // Prune row groups.
        let file_meta = builder.metadata();

        if let Some(pruner) = &self.pruner {
            let selected_row_groups = pruner.prune_row_groups(file_meta)?;
            let row_selection = pruner.prune_pages(file_meta, &selected_row_groups)?;

            builder = builder.with_row_groups(selected_row_groups);
            if let Some(row_selection) = row_selection {
                builder = builder.with_row_selection(row_selection);
            }
        }

        if let Some(predicate) = self.predicate.as_ref() {
            let projection = predicate.projection().clone();
            let predicate = predicate.clone();
            let predicate_fn = move |batch| {
                predicate
                    .evaluate(&batch)
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)))
            };
            builder = builder.with_row_filter(RowFilter::new(vec![Box::new(
                ArrowPredicateFn::new(projection, predicate_fn),
            )]));
        }

        Ok(builder.build()?)
    }

    /// Read a [`DataBlock`] from parquet file using native apache arrow-rs stream API.
    pub async fn read_block_from_stream(
        &self,
        stream: &mut ParquetRecordBatchStream<Reader>,
    ) -> Result<Option<DataBlock>> {
        let record_batch = stream.next().await.transpose()?;

        if let Some(batch) = record_batch {
            let blocks = if self.is_inner_project {
                transform_record_batch(&batch, &self.field_paths)?
            } else {
                let (block, _) = DataBlock::from_record_batch(&batch)?;
                block
            };
            Ok(Some(blocks))
        } else {
            Ok(None)
        }
    }

    /// Read a [`DataBlock`] from parquet file using native apache arrow-rs reader API.
    pub fn read_block(&self, reader: &mut ParquetRecordBatchReader) -> Result<Option<DataBlock>> {
        let record_batch = reader.next().transpose()?;

        if let Some(batch) = record_batch {
            let blocks = if self.is_inner_project {
                transform_record_batch(&batch, &self.field_paths)?
            } else {
                let (block, _) = DataBlock::from_record_batch(&batch)?;
                block
            };
            Ok(Some(blocks))
        } else {
            Ok(None)
        }
    }

    /// Read a [`DataBlock`] from bytes.
    pub fn read_blocks_from_binary(&self, raw: Vec<u8>) -> Result<Vec<DataBlock>> {
        let bytes = Bytes::from(raw);
        let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            bytes,
            ArrowReaderOptions::new()
                .with_page_index(self.need_page_index)
                .with_skip_arrow_metadata(true),
        )?
        .with_projection(self.projection.clone())
        .with_batch_size(self.batch_size);

        // Prune row groups.
        let file_meta = builder.metadata();

        if let Some(pruner) = &self.pruner {
            let selected_row_groups = pruner.prune_row_groups(file_meta)?;
            let row_selection = pruner.prune_pages(file_meta, &selected_row_groups)?;

            builder = builder.with_row_groups(selected_row_groups);
            if let Some(row_selection) = row_selection {
                builder = builder.with_row_selection(row_selection);
            }
        }

        if let Some(predicate) = self.predicate.as_ref() {
            let projection = predicate.projection().clone();
            let predicate = predicate.clone();
            let predicate_fn = move |batch| {
                predicate
                    .evaluate(&batch)
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)))
            };
            builder = builder.with_row_filter(RowFilter::new(vec![Box::new(
                ArrowPredicateFn::new(projection, predicate_fn),
            )]));
        }

        let reader = builder.build()?;
        // Write `if` outside iteration to reduce branches.
        if self.is_inner_project {
            reader
                .into_iter()
                .map(|batch| {
                    let batch = batch?;
                    transform_record_batch(&batch, &self.field_paths)
                })
                .collect()
        } else {
            reader
                .into_iter()
                .map(|batch| {
                    let batch = batch?;
                    Ok(DataBlock::from_record_batch(&batch)?.0)
                })
                .collect()
        }
    }

    /// Read a row group and return a batch record iterator.
    ///
    /// If return [None], it means the whole row group is skipped (by eval push down predicate).
    pub async fn prepare_row_group_reader(
        &self,
        part: &ParquetRSRowGroupPart,
    ) -> Result<Option<ParquetRecordBatchReader>> {
        let page_locations = part.page_locations.as_ref().map(|x| {
            x.iter()
                .map(|x| x.iter().map(PageLocation::from).collect())
                .collect::<Vec<Vec<_>>>()
        });
        let mut row_group = InMemoryRowGroup::new(&part.meta, page_locations.as_deref());
        let mut selection = part
            .selectors
            .as_ref()
            .map(|x| x.iter().map(RowSelector::from).collect::<Vec<_>>())
            .map(RowSelection::from);

        if let Some(predicate) = &self.predicate {
            // Fetch columns used for eval predicate (prewhere).
            row_group
                .fetch(
                    &part.location,
                    self.op.clone(),
                    predicate.projection(),
                    selection.as_ref(),
                )
                .await?;

            let reader = ParquetRecordBatchReader::try_new_with_row_groups(
                predicate.field_levels(),
                &row_group,
                self.batch_size,
                selection.clone(),
            )?;

            let mut filters = vec![];
            for batch in reader {
                let batch = batch?;
                let filter = predicate.evaluate(&batch)?;
                filters.push(filter);
            }
            let sel = RowSelection::from_filters(&filters);
            if !sel.selects_any() {
                // All rows in current row group are filtered out.
                return Ok(None);
            }
            match selection.as_mut() {
                Some(selection) => {
                    selection.and_then(&sel);
                }
                None => {
                    selection = Some(sel);
                }
            }
        }

        // Fetch remain columns.
        row_group
            .fetch(
                &part.location,
                self.op.clone(),
                &self.projection,
                selection.as_ref(),
            )
            .await?;

        let reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.field_levels,
            &row_group,
            self.batch_size,
            selection,
        )?;

        Ok(Some(reader))
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
pub fn transform_record_batch(
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
