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

use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TopKSorter;
use databend_common_metrics::storage::metrics_inc_omit_filter_rowgroups;
use databend_common_metrics::storage::metrics_inc_omit_filter_rows;
use databend_common_storage::OperatorRegistry;
use futures::future::try_join_all;
use opendal::Operator;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::RowGroupMetaData;
use parquet::format::PageLocation;
use parquet::schema::types::SchemaDescPtr;

use crate::parquet_reader::policy::PolicyBuilders;
use crate::parquet_reader::policy::PolicyType;
use crate::parquet_reader::policy::ReadPolicyImpl;
use crate::parquet_reader::policy::POLICY_PREDICATE_ONLY;
use crate::parquet_reader::row_group::InMemoryRowGroup;
use crate::partition::ParquetRowGroupPart;
use crate::read_settings::ReadSettings;
use crate::transformer::RecordBatchTransformer;
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
    sample: None,
});

/// The reader to read a row group.
pub struct RowGroupReader {
    pub(super) ctx: Arc<dyn TableContext>,
    pub(super) op_registry: Arc<dyn OperatorRegistry>,

    pub(super) default_policy: PolicyType,
    pub(super) policy_builders: PolicyBuilders,

    pub(super) schema_desc: SchemaDescPtr,
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
        delete_info: Option<(&ParquetMetaData, &[String])>,
        delete_selection: &mut Option<RowSelection>,
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
        let row_group = InMemoryRowGroup::new(
            path,
            op,
            &part.meta,
            page_locations.as_deref(),
            *read_settings,
        );
        if let (Some((parquet_meta, delete_files)), true) =
            (delete_info, delete_selection.is_none())
        {
            let futures = delete_files.iter().map(|delete| async {
                let (op, path) = self.op_registry.get_operator_path(delete)?;
                let meta = op.stat(path).await?;
                let info = &DELETES_FILE_PUSHDOWN_INFO;

                let mut builder = ParquetReaderBuilder::create(
                    self.ctx.clone(),
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
                    let num_rows = block.num_rows();
                    let column = block.columns()[0].to_column(num_rows);
                    let column = column.as_number().unwrap().as_int64().unwrap();

                    positional_deletes.extend_from_slice(column.as_slice())
                }

                Result::Ok(positional_deletes)
            });
            let positional_deletes = try_join_all(futures)
                .await?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            *delete_selection = Some(Self::build_deletes_row_selection(
                parquet_meta.row_groups(),
                positional_deletes,
            )?);
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
            delete_selection
                .as_mut()
                .map(|selection| selection.split_off(part.meta.num_rows() as usize)),
        ) {
            (Some(result_selection), Some(delete_selection)) => {
                Some(result_selection.intersection(&delete_selection))
            }
            (Some(selection), None) | (None, Some(selection)) => Some(selection),
            (None, None) => None,
        };

        let builder = &self.policy_builders[policy as usize];
        builder
            .fetch_and_build(
                row_group,
                selections,
                topk_sorter,
                self.transformer.clone(),
                self.batch_size,
            )
            .await
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
