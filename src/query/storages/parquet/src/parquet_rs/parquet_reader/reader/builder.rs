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

use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchemaRef;
use opendal::Operator;
use parquet::arrow::arrow_to_parquet_schema;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescPtr;

use super::ParquetRSRowGroupReader;
use crate::parquet_rs::parquet_reader::policy::default_policy_builders;
use crate::parquet_rs::parquet_reader::policy::ReadPolicyBuilder;
use crate::parquet_rs::parquet_reader::policy::POLICY_NO_PREFETCH;
use crate::parquet_rs::parquet_reader::policy::POLICY_PREDICATE_AND_TOPK;
use crate::parquet_rs::parquet_reader::policy::POLICY_PREDICATE_ONLY;
use crate::parquet_rs::parquet_reader::policy::POLICY_TOPK_ONLY;
use crate::parquet_rs::parquet_reader::predicate::build_predicate;
use crate::parquet_rs::parquet_reader::predicate::ParquetPredicate;
use crate::parquet_rs::parquet_reader::topk::build_topk;
use crate::parquet_rs::parquet_reader::topk::BuiltTopK;
use crate::parquet_rs::parquet_reader::utils::compute_output_field_paths;
use crate::parquet_rs::parquet_reader::utils::FieldPaths;
use crate::parquet_rs::parquet_reader::NoPretchPolicyBuilder;
use crate::parquet_rs::parquet_reader::PredicateAndTopkPolicyBuilder;
use crate::parquet_rs::parquet_reader::TopkOnlyPolicyBuilder;
use crate::ParquetRSFullReader;
use crate::ParquetRSPruner;

pub struct ParquetRSReaderBuilder<'a> {
    ctx: Arc<dyn TableContext>,
    op: Operator,
    table_schema: TableSchemaRef,
    schema_desc: SchemaDescPtr,
    arrow_schema: Option<arrow_schema::Schema>,

    push_downs: Option<&'a PushDownInfo>,
    options: ParquetReadOptions,
    pruner: Option<ParquetRSPruner>,
    topk: Option<&'a TopK>,
    partition_columns: Vec<String>,

    // Can be reused to build multiple readers.
    built_predicate: Option<(Arc<ParquetPredicate>, Vec<usize>)>,
    built_topk: Option<BuiltTopK>,
    #[allow(clippy::type_complexity)]
    built_output: Option<(
        ProjectionMask,          // The output projection mask.
        Vec<usize>,              // The column leaves of the projection mask.
        TableSchemaRef,          // The output table schema.
        Arc<Option<FieldPaths>>, /* The output field paths. It's `Some` when the reading is an inner projection. */
    )>,
}

impl<'a> ParquetRSReaderBuilder<'a> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        table_schema: TableSchemaRef,
        arrow_schema: arrow_schema::Schema,
    ) -> Result<ParquetRSReaderBuilder<'a>> {
        let schema_desc = Arc::new(arrow_to_parquet_schema(&arrow_schema)?);
        Ok(Self::create_with_parquet_schema(
            ctx,
            op,
            table_schema,
            schema_desc,
            Some(arrow_schema),
        ))
    }

    pub fn create_with_parquet_schema(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        table_schema: TableSchemaRef,
        schema_desc: SchemaDescPtr,
        arrow_schema: Option<arrow_schema::Schema>,
    ) -> ParquetRSReaderBuilder<'a> {
        ParquetRSReaderBuilder {
            ctx,
            op,
            table_schema,
            schema_desc,
            arrow_schema,
            push_downs: None,
            options: Default::default(),
            pruner: None,
            topk: None,
            built_predicate: None,
            built_topk: None,
            built_output: None,
            partition_columns: vec![],
        }
    }

    pub fn with_push_downs(mut self, push_downs: Option<&'a PushDownInfo>) -> Self {
        self.push_downs = push_downs;
        self
    }

    pub fn with_options(mut self, options: ParquetReadOptions) -> Self {
        self.options = options;
        self
    }

    pub fn with_pruner(mut self, pruner: Option<ParquetRSPruner>) -> Self {
        self.pruner = pruner;
        self
    }

    pub fn with_topk(mut self, topk: Option<&'a TopK>) -> Self {
        self.topk = topk;
        self
    }

    pub fn with_partition_columns(mut self, partition_columns: Vec<String>) -> Self {
        self.partition_columns = partition_columns;
        self
    }

    fn build_predicate(&mut self) -> Result<()> {
        if self.built_predicate.is_some() {
            return Ok(());
        }
        self.built_predicate = PushDownInfo::prewhere_of_push_downs(self.push_downs)
            .map(|prewhere| {
                build_predicate(
                    self.ctx.get_function_context()?,
                    &prewhere,
                    &self.table_schema,
                    &self.schema_desc,
                    &self.partition_columns,
                    self.arrow_schema.as_ref(),
                )
            })
            .transpose()?;
        Ok(())
    }

    fn build_topk(&mut self) -> Result<()> {
        if self.built_topk.is_some() {
            return Ok(());
        }
        self.built_topk = self
            .topk
            .map(|topk| build_topk(topk, &self.schema_desc, self.arrow_schema.as_ref()))
            .transpose()?;
        Ok(())
    }

    pub(crate) fn build_output(&mut self) -> Result<()> {
        if self.built_output.is_some() {
            return Ok(());
        }

        let mut output_projection =
            PushDownInfo::projection_of_push_downs(&self.table_schema, self.push_downs);
        let inner_projection = matches!(output_projection, Projection::InnerColumns(_));

        if let Some(prewhere) = self.push_downs.as_ref().and_then(|p| p.prewhere.as_ref()) {
            output_projection = prewhere.output_columns.clone();
            debug_assert_eq!(
                inner_projection,
                matches!(output_projection, Projection::InnerColumns(_))
            );
        }

        // Build projection mask and field paths for transforming `RecordBatch` to output block.
        // The number of columns in `output_projection` may be less than the number of actual read columns.
        let (projection, output_leaves) = output_projection.to_arrow_projection(&self.schema_desc);
        let output_table_schema = output_projection.project_schema(&self.table_schema);
        let output_field_paths = Arc::new(compute_output_field_paths(
            &self.schema_desc,
            &projection,
            &output_table_schema,
            inner_projection,
        )?);

        self.built_output = Some((
            projection,
            output_leaves,
            Arc::new(output_table_schema),
            output_field_paths,
        ));

        Ok(())
    }

    pub fn build_full_reader(&mut self) -> Result<ParquetRSFullReader> {
        let batch_size = self.ctx.get_settings().get_parquet_max_block_size()? as usize;

        self.build_predicate()?;
        self.build_output()?;

        let predicate = self.built_predicate.as_ref().map(|(pred, _)| pred.clone());
        let (projection, field_paths) = self
            .built_output
            .as_ref()
            .map(|(proj, _, _, paths)| (proj.clone(), paths.clone()))
            .unwrap();

        let (_, _, output_schema, _) = self.built_output.as_ref().unwrap();
        Ok(ParquetRSFullReader {
            op: self.op.clone(),
            output_schema: output_schema.clone(),
            predicate,
            projection,
            field_paths,
            pruner: self.pruner.clone(),
            need_page_index: self.options.prune_pages(),
            batch_size,
        })
    }

    pub fn build_row_group_reader(&mut self) -> Result<ParquetRSRowGroupReader> {
        let batch_size = self.ctx.get_settings().get_max_block_size()? as usize;

        self.build_predicate()?;
        self.build_topk()?;
        self.build_output()?;

        let mut policy_builders = default_policy_builders();
        let default_policy = match (self.built_predicate.as_ref(), self.built_topk.as_ref()) {
            (Some(_), Some(_)) => {
                policy_builders[POLICY_PREDICATE_AND_TOPK as usize] =
                    self.create_predicate_and_topk_policy_builder()?;
                // Predicate may be omitted.
                policy_builders[POLICY_TOPK_ONLY as usize] =
                    self.create_only_topk_policy_builder()?;
                POLICY_PREDICATE_AND_TOPK
            }
            (Some(_), None) => {
                policy_builders[POLICY_PREDICATE_ONLY as usize] =
                    self.create_predicate_and_topk_policy_builder()?;
                // Predicate may be omitted.
                policy_builders[POLICY_NO_PREFETCH as usize] =
                    self.create_no_prefetch_policy_builder()?;
                POLICY_PREDICATE_ONLY
            }
            (None, Some(_)) => {
                policy_builders[POLICY_TOPK_ONLY as usize] =
                    self.create_only_topk_policy_builder()?;
                POLICY_TOPK_ONLY
            }
            (None, None) => {
                policy_builders[POLICY_NO_PREFETCH as usize] =
                    self.create_no_prefetch_policy_builder()?;
                POLICY_NO_PREFETCH
            }
        };

        Ok(ParquetRSRowGroupReader {
            op: self.op.clone(),
            batch_size,
            policy_builders,
            default_policy,
        })
    }

    pub fn create_no_prefetch_policy_builder(&self) -> Result<Box<dyn ReadPolicyBuilder>> {
        let (projection, _, schema, output_field_paths) = self.built_output.as_ref().unwrap();
        let data_schema = DataSchema::from(schema);
        NoPretchPolicyBuilder::create(
            &self.schema_desc,
            self.arrow_schema.as_ref(),
            data_schema,
            projection.clone(),
            output_field_paths.clone(),
        )
    }

    fn create_only_topk_policy_builder(&self) -> Result<Box<dyn ReadPolicyBuilder>> {
        let (_, output_leaves, output_schema, paths) = self.built_output.as_ref().unwrap();
        TopkOnlyPolicyBuilder::create(
            &self.schema_desc,
            self.arrow_schema.as_ref(),
            self.built_topk.as_ref().unwrap(),
            output_schema,
            output_leaves,
            paths.is_some(),
        )
    }

    fn create_predicate_and_topk_policy_builder(&self) -> Result<Box<dyn ReadPolicyBuilder>> {
        let (_, output_leaves, output_schema, _) = self.built_output.as_ref().unwrap();
        let predicate = self.built_predicate.as_ref().unwrap();
        let topk = self.built_topk.as_ref();
        let remain_projection = self
            .push_downs
            .as_ref()
            .and_then(|p| p.prewhere.as_ref())
            .map(|p| &p.remain_columns)
            .unwrap();

        let remain_schema = remain_projection.project_schema(&self.table_schema);

        PredicateAndTopkPolicyBuilder::create(
            &self.schema_desc,
            self.arrow_schema.as_ref(),
            predicate,
            topk,
            output_leaves,
            &remain_schema,
            output_schema,
        )
    }
}
