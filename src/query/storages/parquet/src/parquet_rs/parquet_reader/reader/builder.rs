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

use common_catalog::plan::ParquetReadOptions;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::TopK;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use opendal::Operator;
use parquet::arrow::arrow_to_parquet_schema;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescPtr;

use super::ParquetRSRowGroupReader;
use crate::parquet_rs::parquet_reader::policy::default_policy_builders;
use crate::parquet_rs::parquet_reader::policy::PolicyType;
use crate::parquet_rs::parquet_reader::policy::ReadPolicyBuilder;
use crate::parquet_rs::parquet_reader::policy::POLICY_NO_PREFETCH;
use crate::parquet_rs::parquet_reader::policy::POLICY_PREDICATE_AND_TOPK;
use crate::parquet_rs::parquet_reader::policy::POLICY_PREDICATE_ONLY;
use crate::parquet_rs::parquet_reader::policy::POLICY_TOPK_ONLY;
use crate::parquet_rs::parquet_reader::predicate::build_predicate;
use crate::parquet_rs::parquet_reader::predicate::ParquetPredicate;
use crate::parquet_rs::parquet_reader::topk::build_topk;
use crate::parquet_rs::parquet_reader::topk::ParquetTopK;
use crate::parquet_rs::parquet_reader::utils::compute_output_field_paths;
use crate::parquet_rs::parquet_reader::utils::to_arrow_schema;
use crate::parquet_rs::parquet_reader::utils::FieldPaths;
use crate::parquet_rs::parquet_reader::NoPretchPolicyBuilder;
use crate::parquet_rs::parquet_reader::TopkOnlyPolicyBuilder;
use crate::ParquetFSFullReader;
use crate::ParquetRSPruner;

pub struct ParquetRSReaderBuilder<'a> {
    ctx: Arc<dyn TableContext>,
    op: Operator,
    table_schema: TableSchemaRef,
    schema_desc: SchemaDescPtr,

    push_downs: Option<&'a PushDownInfo>,
    options: ParquetReadOptions,
    pruner: Option<ParquetRSPruner>,
    topk: Option<&'a TopK>,

    // Can be reused to build multiple readers.
    built_predicate: Option<(Arc<ParquetPredicate>, Vec<usize>, Projection)>,
    built_topk: Option<(Arc<ParquetTopK>, TableField)>,
    #[allow(clippy::type_complexity)]
    built_output: Option<(
        ProjectionMask,
        Vec<usize>,
        TableSchemaRef,
        Arc<Option<FieldPaths>>,
    )>,
}

impl<'a> ParquetRSReaderBuilder<'a> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        table_schema: TableSchemaRef,
        arrow_schema: &arrow_schema::Schema,
    ) -> Result<ParquetRSReaderBuilder<'a>> {
        let schema_desc = Arc::new(arrow_to_parquet_schema(arrow_schema)?);
        Ok(Self::create_with_parquet_schema(
            ctx,
            op,
            table_schema,
            schema_desc,
        ))
    }

    pub fn create_with_parquet_schema(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        table_schema: TableSchemaRef,
        schema_desc: SchemaDescPtr,
    ) -> ParquetRSReaderBuilder<'a> {
        ParquetRSReaderBuilder {
            ctx,
            op,
            table_schema,
            schema_desc,
            push_downs: None,
            options: Default::default(),
            pruner: None,
            topk: None,
            built_predicate: None,
            built_topk: None,
            built_output: None,
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

    fn build_predicate(&mut self) -> Result<()> {
        if self.built_predicate.is_some() {
            return Ok(());
        }
        let predicate = PushDownInfo::prewhere_of_push_downs(self.push_downs)
            .map(|prewhere| {
                build_predicate(
                    self.ctx.get_function_context()?,
                    &prewhere,
                    &self.table_schema,
                    &self.schema_desc,
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
            .map(|topk| build_topk(topk, &self.schema_desc))
            .transpose()?;
        Ok(())
    }

    fn build_output(&mut self, output_projection: Projection) -> Result<()> {
        if self.built_output.is_some() {
            return Ok(());
        }

        // Build projection mask and field paths for transforming `RecordBatch` to output block.
        // The number of columns in `output_projection` may be less than the number of actual read columns.
        let inner_projection = matches!(output_projection, Projection::InnerColumns(_));
        let (projection, output_leaves) = output_projection.to_arrow_projection(&self.schema_desc);
        let output_table_schema = output_projection.project_schema(&self.table_schema);
        let output_arrow_schema = to_arrow_schema(&output_table_schema);

        let field_levels =
            parquet_to_arrow_field_levels(&self.schema_desc, projection.clone(), None)?;

        let output_field_paths = Arc::new(compute_output_field_paths(
            &self.schema_desc,
            &projection,
            &output_arrow_schema,
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

    pub fn build_full_reader(&mut self) -> Result<ParquetFSFullReader> {
        let mut output_projection =
            PushDownInfo::projection_of_push_downs(&self.table_schema, self.push_downs);
        let inner_projection = matches!(output_projection, Projection::InnerColumns(_));

        self.build_predicate()?;
        if let Some((_, _, proj)) = self.built_predicate.as_ref() {
            output_projection = proj.clone();
            debug_assert_eq!(
                matches!(output_projection, Projection::InnerColumns(_)),
                inner_projection
            )
        }
        let predicate = self
            .built_predicate
            .as_ref()
            .map(|(pred, _, _)| pred.clone());

        let batch_size = self.ctx.get_settings().get_max_block_size()? as usize;

        self.build_output(output_projection)?;
        let (projection, field_paths) = self
            .built_output
            .as_ref()
            .map(|(proj, _, _, paths)| (proj.clone(), paths.clone()))
            .unwrap();

        Ok(ParquetFSFullReader {
            op: self.op.clone(),
            predicate,
            projection,
            field_paths,
            pruner: self.pruner.clone(),
            need_page_index: self.options.prune_pages(),
            batch_size,
        })
    }

    pub fn build_row_group_reader(&mut self) -> Result<ParquetRSRowGroupReader> {
        let mut output_projection =
            PushDownInfo::projection_of_push_downs(&self.table_schema, self.push_downs);
        let inner_projection = matches!(output_projection, Projection::InnerColumns(_));

        self.build_predicate()?;
        if let Some((_, _, proj)) = self.built_predicate.as_ref() {
            output_projection = proj.clone();
            debug_assert_eq!(
                matches!(output_projection, Projection::InnerColumns(_)),
                inner_projection
            )
        }
        let predicate = self
            .built_predicate
            .as_ref()
            .map(|(pred, leaves, _)| (pred.clone(), leaves.clone()));

        self.build_topk()?;
        let topk = self.built_topk.clone();

        let batch_size = self.ctx.get_settings().get_max_block_size()? as usize;

        self.build_output(output_projection)?;
        let (projection, output_leaves, output_table_schema, output_field_paths) =
            self.built_output.clone().unwrap();

        let mut policy_builders = default_policy_builders();
        let default_policy = match (predicate, topk) {
            (Some(pred), Some(topk)) => {
                policy_builders[POLICY_PREDICATE_AND_TOPK as usize] =
                    self.add_predicate_and_topk_policy_builder(POLICY_PREDICATE_AND_TOPK)?;
                // Predicate may be omitted.
                policy_builders[POLICY_TOPK_ONLY as usize] = self.add_only_topk_policy_builder(
                    &output_leaves,
                    &output_table_schema,
                    topk,
                    inner_projection,
                )?;
                POLICY_PREDICATE_AND_TOPK
            }
            (Some(pred), None) => {
                policy_builders[POLICY_PREDICATE_ONLY as usize] =
                    self.add_predicate_and_topk_policy_builder(POLICY_PREDICATE_ONLY)?;
                // Predicate may be omitted.
                policy_builders[POLICY_NO_PREFETCH as usize] = NoPretchPolicyBuilder::create(
                    &self.schema_desc,
                    projection,
                    output_field_paths,
                )?;
                POLICY_PREDICATE_ONLY
            }
            (None, Some(topk)) => {
                policy_builders[POLICY_TOPK_ONLY as usize] = self.add_only_topk_policy_builder(
                    &output_leaves,
                    &output_table_schema,
                    topk,
                    inner_projection,
                )?;
                POLICY_TOPK_ONLY
            }
            (None, None) => {
                policy_builders[POLICY_NO_PREFETCH as usize] = NoPretchPolicyBuilder::create(
                    &self.schema_desc,
                    projection,
                    output_field_paths,
                )?;
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

    fn add_only_topk_policy_builder(
        &self,
        output_leaves: &[usize],
        output_schema: &TableSchema,
        topk: (Arc<ParquetTopK>, TableField),
        inner_projection: bool,
    ) -> Result<Box<dyn ReadPolicyBuilder>> {
        let remain_leaves = output_leaves
            .iter()
            .cloned()
            .filter(|i| *i != topk.1.column_id as usize)
            .collect::<Vec<_>>();
        let remain_projection = ProjectionMask::leaves(&self.schema_desc, remain_leaves);
        TopkOnlyPolicyBuilder::create(
            &self.schema_desc,
            topk,
            remain_projection,
            output_schema,
            inner_projection,
        )
    }

    fn add_predicate_and_topk_policy_builder(
        &self,
        policy: PolicyType,
    ) -> Result<Box<dyn ReadPolicyBuilder>> {
        todo!()
    }
}
