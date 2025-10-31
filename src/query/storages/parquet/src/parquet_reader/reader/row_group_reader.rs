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
use std::sync::LazyLock;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TopKSorter;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::metrics_inc_omit_filter_rowgroups;
use databend_common_metrics::storage::metrics_inc_omit_filter_rows;
use databend_common_storage::OperatorRegistry;
use futures::future::try_join_all;
use futures::StreamExt;
use opendal::Operator;
use opendal::Reader;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::RowGroupMetaData;
use parquet::format::PageLocation;
use parquet::schema::types::SchemaDescPtr;

use crate::parquet_part::DeleteTask;
use crate::parquet_reader::policy::PolicyBuilders;
use crate::parquet_reader::policy::PolicyType;
use crate::parquet_reader::policy::ReadPolicyImpl;
use crate::parquet_reader::policy::POLICY_PREDICATE_ONLY;
use crate::parquet_reader::predicate::build_predicate;
use crate::parquet_reader::predicate::ParquetPredicate;
use crate::parquet_reader::row_group::InMemoryRowGroup;
use crate::partition::ParquetRowGroupPart;
use crate::read_settings::ReadSettings;
use crate::transformer::RecordBatchTransformer;
use crate::DeleteType;
use crate::ParquetFileReader;
use crate::ParquetReaderBuilder;
use crate::ParquetSourceType;

static DELETES_FILE_SCHEMA: LazyLock<arrow_schema::Schema> = LazyLock::new(|| {
    arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false),
    ])
});

static DELETES_FILE_TABLE_SCHEMA: LazyLock<Arc<TableSchema>> = LazyLock::new(|| {
    Arc::new(TableSchema::new(vec![
        TableField::new("file_path", TableDataType::String),
        TableField::new("pos", TableDataType::Number(NumberDataType::Int64)),
    ]))
});

static DELETES_FILE_PUSHDOWN_INFO: LazyLock<PushDownInfo> = LazyLock::new(|| PushDownInfo {
    projection: Some(Projection::Columns(vec![1])),
    output_columns: None,
    filters: None,
    is_deterministic: false,
    prewhere: None,
    limit: None,
    order_by: vec![],
    virtual_column: None,
    lazy_materialization: false,
    agg_index: None,
    change_type: None,
    inverted_index: None,
    vector_index: None,
    sample: None,
});

enum DeleteResult {
    Positions(Vec<i64>),
    Equality {
        predicate: Expr<String>,
        related_columns: Vec<FieldIndex>,
    },
}

#[derive(Debug)]
pub struct DeleteFilter {
    row_selection: RowSelection,
    predicate: Arc<ParquetPredicate>,
}

/// The reader to read a row group.
pub struct RowGroupReader {
    pub(super) ctx: Arc<dyn TableContext>,
    pub(super) op_registry: Arc<dyn OperatorRegistry>,

    pub(super) default_policy: PolicyType,
    pub(super) policy_builders: PolicyBuilders,

    pub(super) table_schema: TableSchemaRef,
    pub(super) schema_desc: SchemaDescPtr,
    pub(super) arrow_schema: Option<arrow_schema::Schema>,
    pub(super) partition_columns: Vec<String>,
    pub(super) transformer: Option<RecordBatchTransformer>,
    // Options
    pub(super) batch_size: usize,
}

impl RowGroupReader {
    pub fn operator<'a>(&self, location: &'a str) -> Result<(Operator, &'a str)> {
        Ok(self.op_registry.get_operator_path(location)?)
    }

    pub fn schema_desc(&self) -> &SchemaDescPtr {
        &self.schema_desc
    }

    /// Read a row group and return a reader with certain policy.
    /// If return [None], it means the whole row group is skipped (by eval push down predicate).
    pub async fn create_read_policy(
        &self,
        read_settings: &ReadSettings,
        part: &ParquetRowGroupPart,
        topk_sorter: &mut Option<TopKSorter>,
        delete_info: Option<(&ParquetMetaData, &[DeleteTask])>,
        delete_filter: &mut Option<DeleteFilter>,
    ) -> Result<Option<ReadPolicyImpl>> {
        if let Some((sorter, min_max)) = topk_sorter.as_ref().zip(part.sort_min_max.as_ref()) {
            if sorter.never_match(min_max) {
                return Ok(None);
            }
        }
        let page_locations = part.page_locations.as_ref().map(|x| {
            x.iter()
                .map(|x| x.iter().map(PageLocation::from).collect())
                .collect::<Vec<Vec<_>>>()
        });
        let (op, path) = self.operator(&part.location)?;
        let row_group = InMemoryRowGroup::new(path, op, &part.meta, page_locations, *read_settings);
        if let (Some((parquet_meta, delete_files)), true) = (delete_info, delete_filter.is_none()) {
            let futures = delete_files.iter().map(|delete| async {
                let (op, path) = self.op_registry.get_operator_path(&delete.path)?;
                match delete.ty {
                    DeleteType::Position => {
                        let positional_deletes =
                            Self::load_position_deletes(self.ctx.clone(), op, path).await?;
                        Result::Ok(DeleteResult::Positions(positional_deletes))
                    }
                    DeleteType::Equality => {
                        let (predicate, related_columns) = Self::load_equality_deletes(
                            &self.ctx,
                            &self.table_schema,
                            &delete.equality_ids,
                            op,
                            path,
                        )
                        .await?;
                        Result::Ok(DeleteResult::Equality {
                            predicate,
                            related_columns,
                        })
                    }
                }
            });

            let mut positional_deletes = Vec::new();
            let mut equality_expr = Expr::Constant(Constant {
                span: None,
                scalar: Scalar::Boolean(true),
                data_type: DataType::Boolean,
            });

            let mut prewhere_columns = Vec::new();
            for result in try_join_all(futures).await? {
                match result {
                    DeleteResult::Positions(positions) => {
                        positional_deletes.extend_from_slice(&positions);
                    }
                    DeleteResult::Equality {
                        predicate,
                        related_columns,
                    } => {
                        equality_expr = check_function(
                            None,
                            "and",
                            &[],
                            &[equality_expr, predicate],
                            &BUILTIN_FUNCTIONS,
                        )?;
                        prewhere_columns.extend_from_slice(&related_columns);
                    }
                }
            }
            prewhere_columns.sort();
            positional_deletes.sort();

            let prewhere_info = PrewhereInfo {
                output_columns: Projection::Columns(prewhere_columns.clone()),
                prewhere_columns: Projection::Columns(prewhere_columns),
                remain_columns: Projection::Columns(vec![]),
                filter: equality_expr.as_remote_expr(),
                virtual_column_ids: None,
            };
            let row_selection =
                Self::build_deletes_row_selection(parquet_meta.row_groups(), positional_deletes)?;
            let (predicate, _) = build_predicate(
                self.ctx.get_function_context()?,
                &prewhere_info,
                &self.table_schema,
                &self.schema_desc,
                &self.partition_columns,
                self.arrow_schema.as_ref(),
            )?;
            *delete_filter = Some(DeleteFilter {
                row_selection,
                predicate,
            });
        }
        let mut part_selection = part
            .selectors
            .as_ref()
            .map(|x| x.iter().map(RowSelector::from).collect::<Vec<_>>())
            .map(RowSelection::from);

        let mut policy = self.default_policy;
        if part.omit_filter {
            // Remove predicate.
            // PRED_ONLY (0b01) -> NO_PREFETCH (0b00)
            // PRED_AND_TOPK (0b11) -> TOPK_ONLY (0b10)
            policy &= !POLICY_PREDICATE_ONLY;
            part_selection = None;
            metrics_inc_omit_filter_rowgroups(1);
            metrics_inc_omit_filter_rows(row_group.row_count() as u64);
        }

        let selections = match (
            part_selection,
            // The Pos in DeleteFile is global in Parquet.
            // Therefore, it is necessary to split it in units of RowGroup.
            delete_filter.as_mut().map(|selection| {
                selection
                    .row_selection
                    .split_off(part.meta.num_rows() as usize)
            }),
        ) {
            (Some(result_selection), Some(delete_selection)) => {
                Some(result_selection.intersection(&delete_selection))
            }
            (Some(selection), None) | (None, Some(selection)) => Some(selection),
            (None, None) => None,
        };
        let filter = delete_filter
            .as_ref()
            .map(|filter| filter.predicate.clone());

        let builder = &self.policy_builders[policy as usize];
        builder
            .fetch_and_build(
                row_group,
                selections,
                topk_sorter,
                self.transformer.clone(),
                self.batch_size,
                filter,
            )
            .await
    }

    async fn load_position_deletes(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        path: &str,
    ) -> Result<Vec<i64>> {
        let meta = op.stat(path).await?;
        let info = &DELETES_FILE_PUSHDOWN_INFO;

        let mut builder = ParquetReaderBuilder::create(
            ctx,
            Arc::new(op),
            DELETES_FILE_TABLE_SCHEMA.clone(),
            DELETES_FILE_SCHEMA.clone(),
        )?
        .with_push_downs(Some(info));
        let reader = builder.build_full_reader(ParquetSourceType::Iceberg, false)?;
        let mut stream = reader
            .prepare_data_stream(path, meta.content_length(), None)
            .await?;
        let mut positional_deletes = Vec::new();

        while let Some(block) = reader.read_block_from_stream(&mut stream).await? {
            let view = block.get_by_offset(0).downcast::<Int64Type>().unwrap();
            positional_deletes.extend(view.iter())
        }
        Ok(positional_deletes)
    }

    async fn load_equality_deletes(
        ctx: &Arc<dyn TableContext>,
        table_schema: &TableSchema,
        equality_ids: &[i32],
        op: Operator,
        path: &str,
    ) -> Result<(Expr<String>, Vec<FieldIndex>)> {
        let batch_size = ctx.get_settings().get_parquet_max_block_size()? as usize;
        let meta = op.stat(path).await?;
        let reader: Reader = op.reader(path).await?;
        let reader = ParquetFileReader::new(reader, meta.content_length());
        let mut stream =
            ParquetRecordBatchStreamBuilder::new_with_options(reader, ArrowReaderOptions::new())
                .await?
                .with_batch_size(batch_size)
                .build()?;

        let mut predicates = Vec::new();
        let fn_field_id = |field: &FieldRef| {
            field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|value| value.parse::<i32>().ok())
        };

        let mut related_columns = Vec::new();
        while let Some(record_batch) = stream.next().await {
            let record_batch = record_batch?;

            if record_batch.num_columns() == 0 {
                return Ok((
                    Expr::Constant(Constant {
                        span: None,
                        scalar: Scalar::Boolean(true),
                        data_type: DataType::Boolean,
                    }),
                    Vec::new(),
                ));
            }

            let fields_with_columns = record_batch
                .schema_ref()
                .fields()
                .iter()
                .zip(record_batch.columns().iter())
                .filter(|(field, _)| {
                    fn_field_id(field).map(|field_id| equality_ids.contains(&field_id))
                        == Some(true)
                })
                .collect::<Vec<_>>();

            for (field, array) in fields_with_columns.iter() {
                let table_field = TableField::try_from(field.as_ref())?;
                let (field_index, _) = table_schema
                    .column_with_name(table_field.name())
                    .ok_or_else(|| {
                        ErrorCode::UnknownColumn(format!(
                            "Unknown column name: {}",
                            table_field.name()
                        ))
                    })?;
                related_columns.push(field_index);

                let data_type = DataType::from(table_field.data_type());
                let column = Value::from_arrow_rs(ArrayRef::clone(array), &data_type)?;

                for i in 0..column.len() {
                    let Some(scala) = column.index(i) else { break };
                    let column_name = table_field.name().to_string();
                    let column_expr = Expr::ColumnRef(ColumnRef {
                        span: None,
                        id: column_name.clone(),
                        data_type: data_type.clone(),
                        display_name: column_name,
                    });
                    let scalar_ty = scala.infer_data_type();
                    let scalar_expr = Expr::Constant(Constant {
                        span: None,
                        scalar: scala.to_owned(),
                        data_type: scalar_ty.clone(),
                    });
                    let eq_expr = check_function(
                        None,
                        "eq",
                        &[],
                        &[column_expr, scalar_expr],
                        &BUILTIN_FUNCTIONS,
                    )?;
                    predicates.push(check_function(
                        None,
                        "not",
                        &[],
                        &[eq_expr],
                        &BUILTIN_FUNCTIONS,
                    )?);
                }
            }
        }
        let combined_predicate = predicates.into_iter().try_fold(
            Expr::Constant(Constant {
                span: None,
                scalar: Scalar::Boolean(true),
                data_type: DataType::Boolean,
            }),
            |acc, expr| check_function(None, "and", &[], &[acc, expr], &BUILTIN_FUNCTIONS),
        )?;

        Ok((
            check_function(
                None,
                "is_true",
                &[],
                &[combined_predicate],
                &BUILTIN_FUNCTIONS,
            )?,
            related_columns,
        ))
    }

    fn build_deletes_row_selection(
        row_group_metadata_list: &[RowGroupMetaData],
        positional_deletes: Vec<i64>,
    ) -> Result<RowSelection> {
        debug_assert!(positional_deletes.is_sorted());
        let mut results: Vec<RowSelector> = Vec::new();
        let mut current_row_group_base_idx: u64 = 0;
        let mut delete_vector_iter = positional_deletes.into_iter().map(|i| i as u64);
        let mut next_deleted_row_idx_opt = delete_vector_iter.next();

        for row_group_metadata in row_group_metadata_list.iter() {
            let row_group_num_rows = row_group_metadata.num_rows() as u64;
            let next_row_group_base_idx = current_row_group_base_idx + row_group_num_rows;

            let mut next_deleted_row_idx = match next_deleted_row_idx_opt {
                Some(next_deleted_row_idx) => {
                    // if the index of the next deleted row is beyond this row group, add a selection for
                    // the remainder of this row group and skip to the next row group
                    if next_deleted_row_idx >= next_row_group_base_idx {
                        results.push(RowSelector::select(row_group_num_rows as usize));
                        continue;
                    }

                    next_deleted_row_idx
                }

                // If there are no more pos deletes, add a selector for the entirety of this row group.
                _ => {
                    results.push(RowSelector::select(row_group_num_rows as usize));
                    continue;
                }
            };

            let mut current_idx = current_row_group_base_idx;
            'chunks: while next_deleted_row_idx < next_row_group_base_idx {
                // `select` all rows that precede the next delete index
                if current_idx < next_deleted_row_idx {
                    let run_length = next_deleted_row_idx - current_idx;
                    results.push(RowSelector::select(run_length as usize));
                    current_idx += run_length;
                }

                // `skip` all consecutive deleted rows in the current row group
                let mut run_length = 0;
                while next_deleted_row_idx == current_idx
                    && next_deleted_row_idx < next_row_group_base_idx
                {
                    run_length += 1;
                    current_idx += 1;

                    next_deleted_row_idx_opt = delete_vector_iter.next();
                    next_deleted_row_idx = match next_deleted_row_idx_opt {
                        Some(next_deleted_row_idx) => next_deleted_row_idx,
                        _ => {
                            // We've processed the final positional delete.
                            // Conclude the skip and then break so that we select the remaining
                            // rows in the row group and move on to the next row group
                            results.push(RowSelector::skip(run_length));
                            break 'chunks;
                        }
                    };
                }
                if run_length > 0 {
                    results.push(RowSelector::skip(run_length));
                }
            }

            if current_idx < next_row_group_base_idx {
                results.push(RowSelector::select(
                    (next_row_group_base_idx - current_idx) as usize,
                ));
            }

            current_row_group_base_idx += row_group_num_rows;
        }

        Ok(RowSelection::from(results))
    }
}
