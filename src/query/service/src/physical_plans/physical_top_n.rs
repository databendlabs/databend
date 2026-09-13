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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_common_pipeline_transforms::sorts::core::RowConverter;
use databend_common_pipeline_transforms::sorts::core::Rows;
use databend_common_pipeline_transforms::sorts::core::RowsTypeVisitor;
use databend_common_pipeline_transforms::sorts::core::SortKeyDescription;
use databend_common_pipeline_transforms::sorts::core::select_row_type;
use databend_common_sql::ColumnSet;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_storages_parquet::ReadSettings;
use itertools::Itertools;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::FinalTopNFormatter;
use crate::physical_plans::format::PartialTopNFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::runtime_scan_filter::register_runtime_top_n_filter;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::top_n::TransformFinalTopN;
use crate::pipelines::processors::transforms::top_n::TransformPartialTopN;
use crate::sessions::QueryContext;

/// Per-stream TopN candidate selection. It consumes unsorted input and emits
/// at most `candidate_count` internally sorted rows with an order column.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PartialTopNPlan {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub order_by: Vec<SortDesc>,
    pub candidate_count: usize,
    pub enable_fixed_rows: bool,

    // Only used for explain.
    pub stat_info: Option<PlanStatsInfo>,
}

/// Global TopN selection. It consumes sorted candidate blocks, applies the
/// final offset/limit and removes the internal order column.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FinalTopNPlan {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub order_by: Vec<SortDesc>,
    pub limit: usize,
    pub offset: usize,
    pub enable_fixed_rows: bool,

    // Only used for explain.
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for PartialTopNPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let key_desc = SortKeyDescription::new(
            sort_column_descriptions(&self.order_by, &input_schema)?.into(),
            input_schema,
            self.enable_fixed_rows,
        )?;
        Ok(key_desc.schema_with_order_col())
    }

    fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(PartialTopNFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(format!(
            "{}; candidates {}",
            format_order_by(&self.order_by),
            self.candidate_count
        ))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::from([(
            String::from("Number of candidates"),
            vec![self.candidate_count.to_string()],
        )]))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        PhysicalPlan::new(PartialTopNPlan {
            meta: self.meta.clone(),
            input: children.pop().unwrap(),
            order_by: self.order_by.clone(),
            candidate_count: self.candidate_count,
            enable_fixed_rows: self.enable_fixed_rows,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let runtime_top_n_filter = register_runtime_top_n_filter(
            &builder.ctx,
            &self.input,
            &self.order_by,
            self.candidate_count,
        );

        self.input.build_pipeline(builder)?;

        let input_schema = self.input.output_schema()?;
        let key_desc = SortKeyDescription::new(
            sort_column_descriptions(&self.order_by, &input_schema)?.into(),
            input_schema,
            self.enable_fixed_rows,
        )?;
        let spill_schema = key_desc.schema_with_order_col();
        let max_threads = builder.settings.get_max_threads()? as usize;
        if builder.main_pipeline.output_len() == 1 || max_threads == 1 {
            builder.main_pipeline.try_resize(max_threads)?;
        }

        let max_block_size = builder.settings.get_max_block_size()? as usize;
        let memory_settings = MemorySettings::from_sort_settings(&builder.ctx)?;
        let writer_pool_bytes = builder
            .settings
            .get_spill_writer_memory_pool_size_mb()?
            .saturating_mul(1024 * 1024);
        let filter =
            runtime_top_n_filter.map(|filter| (key_desc.sort_column_desc()[0].offset, filter));
        let params = PartialTopNParams {
            ctx: builder.ctx.clone(),
            key_desc,
            candidate_count: self.candidate_count,
            max_block_size,
            memory_settings,
            spill_schema,
            writer_pool_bytes,
            read_settings: ReadSettings::from_settings(&builder.settings)?,
            enable_fixed_rows: self.enable_fixed_rows,
            runtime_top_n_filter: filter,
        };

        builder
            .main_pipeline
            .add_transform(|input, output| params.build(input, output))
    }
}

#[typetag::serde]
impl IPhysicalPlan for FinalTopNPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        candidate_input_schema(
            &self.order_by,
            self.input.output_schema()?,
            self.enable_fixed_rows,
        )
    }

    fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(FinalTopNFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(format!(
            "{}; limit {}; offset {}",
            format_order_by(&self.order_by),
            self.limit,
            self.offset
        ))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::from([
            (String::from("Number of rows"), vec![self.limit.to_string()]),
            (String::from("Offset"), vec![self.offset.to_string()]),
        ]))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        PhysicalPlan::new(FinalTopNPlan {
            meta: self.meta.clone(),
            input: children.pop().unwrap(),
            order_by: self.order_by.clone(),
            limit: self.limit,
            offset: self.offset,
            enable_fixed_rows: self.enable_fixed_rows,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let input_schema = self.input.output_schema()?;
        let schema =
            candidate_input_schema(&self.order_by, input_schema.clone(), self.enable_fixed_rows)?;
        let key_desc = SortKeyDescription::new(
            sort_column_descriptions(&self.order_by, &input_schema)?.into(),
            schema,
            self.enable_fixed_rows,
        )?;
        let max_block_size = builder.settings.get_max_block_size()? as usize;
        let params = FinalTopNParams {
            key_desc,
            limit: self.limit,
            offset: self.offset,
            max_block_size,
            enable_fixed_rows: self.enable_fixed_rows,
        };

        builder.main_pipeline.try_resize(1)?;
        builder
            .main_pipeline
            .add_transform(|input, output| params.build(input, output))
    }
}

fn sort_column_descriptions(
    order_by: &[SortDesc],
    schema: &DataSchema,
) -> Result<Vec<SortColumnDescription>> {
    order_by
        .iter()
        .map(|desc| {
            Ok(SortColumnDescription {
                offset: schema.index_of(&desc.order_by.to_string())?,
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            })
        })
        .collect()
}

fn candidate_input_schema(
    order_by: &[SortDesc],
    schema: DataSchemaRef,
    enable_fixed_rows: bool,
) -> Result<DataSchemaRef> {
    SortKeyDescription::strip_order_col_schema(
        sort_column_descriptions(order_by, &schema)?.into(),
        schema,
        enable_fixed_rows,
    )
}

fn format_order_by(order_by: &[SortDesc]) -> String {
    order_by
        .iter()
        .map(|desc| {
            format!(
                "{}{}{}",
                desc.display_name,
                if desc.asc { "" } else { " DESC" },
                if desc.nulls_first { " NULLS FIRST" } else { "" }
            )
        })
        .join(", ")
}

/// Everything the partial TopN stage needs besides the ports. Packed once at
/// pipeline build; `build` selects the concrete row type and wires the
/// processor, mirroring `TransformSortBuilder`.
struct PartialTopNParams {
    ctx: Arc<QueryContext>,
    key_desc: SortKeyDescription,
    candidate_count: usize,
    max_block_size: usize,
    memory_settings: MemorySettings,
    spill_schema: DataSchemaRef,
    writer_pool_bytes: usize,
    read_settings: ReadSettings,
    enable_fixed_rows: bool,
    runtime_top_n_filter: Option<(usize, Arc<RuntimeTopNFilter>)>,
}

impl PartialTopNParams {
    fn build(&self, input: Arc<InputPort>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        struct Build<'a> {
            params: &'a PartialTopNParams,
            input: Arc<InputPort>,
            output: Arc<OutputPort>,
        }

        impl RowsTypeVisitor for Build<'_> {
            type Result = Result<ProcessorPtr>;

            fn sort_key_desc(&self) -> SortKeyDescription {
                self.params.key_desc.clone()
            }

            fn visit_type<R>(&mut self) -> Self::Result
            where
                R: Rows + 'static,
                R::Converter: Send + 'static,
            {
                let params = self.params;
                let row_converter = if params.key_desc.uses_source_sort_col() {
                    None
                } else {
                    Some(R::Converter::new(params.key_desc.clone())?)
                };
                let transform = TransformPartialTopN::<R>::new(
                    params.ctx.clone(),
                    params.candidate_count,
                    row_converter,
                    params.key_desc.sort_row_offset(),
                    params.max_block_size,
                    params.memory_settings.clone(),
                    params.spill_schema.clone(),
                    params.writer_pool_bytes,
                    params.read_settings,
                    params.runtime_top_n_filter.clone(),
                );
                Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                    self.input.clone(),
                    self.output.clone(),
                    transform,
                )))
            }
        }

        let mut build = Build {
            params: self,
            input,
            output,
        };
        select_row_type(&mut build, self.enable_fixed_rows)
    }
}

/// See [`PartialTopNParams`].
struct FinalTopNParams {
    key_desc: SortKeyDescription,
    limit: usize,
    offset: usize,
    max_block_size: usize,
    enable_fixed_rows: bool,
}

impl FinalTopNParams {
    fn build(&self, input: Arc<InputPort>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        struct Build<'a> {
            params: &'a FinalTopNParams,
            input: Arc<InputPort>,
            output: Arc<OutputPort>,
        }

        impl RowsTypeVisitor for Build<'_> {
            type Result = Result<ProcessorPtr>;

            fn sort_key_desc(&self) -> SortKeyDescription {
                self.params.key_desc.clone()
            }

            fn visit_type<R>(&mut self) -> Self::Result
            where
                R: Rows + 'static,
                R::Converter: Send + 'static,
            {
                let params = self.params;
                let transform = TransformFinalTopN::<R>::new(
                    params.limit,
                    params.offset,
                    !params.key_desc.uses_source_sort_col(),
                    params.key_desc.sort_row_offset(),
                    params.max_block_size,
                );
                Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                    self.input.clone(),
                    self.output.clone(),
                    transform,
                )))
            }
        }

        let mut build = Build {
            params: self,
            input,
            output,
        };
        select_row_type(&mut build, self.enable_fixed_rows)
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_top_n(
        &mut self,
        s_expr: &SExpr,
        top_n: &databend_common_sql::plans::TopN,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let apply_lazy_materialization = top_n.after_exchange != Some(false)
            && !top_n.lazy_columns.is_empty()
            && s_expr.unary_child().support_lazy_materialize();

        if apply_lazy_materialization {
            required = required.difference(&top_n.lazy_columns).copied().collect();
            required.extend(self.metadata.read().row_id_indexes());
        }
        required.extend(top_n.items.iter().map(|item| item.index));

        let order_by = top_n
            .items
            .iter()
            .map(|item| SortDesc {
                asc: item.asc,
                nulls_first: item.nulls_first,
                order_by: item.index,
                display_name: self.metadata.read().column(item.index).name(),
            })
            .collect::<Vec<_>>();
        let enable_fixed_rows = self.ctx.get_settings().get_enable_fixed_rows_sort()?;
        let input = self.build(s_expr.unary_child(), required).await?;

        let plan = match top_n.after_exchange {
            Some(false) => PhysicalPlan::new(PartialTopNPlan {
                meta: PhysicalPlanMeta::new("PartialTopN"),
                input,
                order_by,
                candidate_count: top_n.candidate_count(),
                enable_fixed_rows,
                stat_info: Some(stat_info.clone()),
            }),
            Some(true) => PhysicalPlan::new(FinalTopNPlan {
                meta: PhysicalPlanMeta::new("FinalTopN"),
                input,
                order_by,
                limit: top_n.limit,
                offset: top_n.offset,
                enable_fixed_rows,
                stat_info: Some(stat_info.clone()),
            }),
            None => {
                let input_stats = self.build_plan_stat_info(s_expr.unary_child())?;
                let partial_stats = PlanStatsInfo {
                    estimated_rows: input_stats
                        .estimated_rows
                        .min(top_n.candidate_count() as f64),
                };
                let partial = PhysicalPlan::new(PartialTopNPlan {
                    meta: PhysicalPlanMeta::new("PartialTopN"),
                    input,
                    order_by: order_by.clone(),
                    candidate_count: top_n.candidate_count(),
                    enable_fixed_rows,
                    stat_info: Some(partial_stats),
                });
                PhysicalPlan::new(FinalTopNPlan {
                    meta: PhysicalPlanMeta::new("FinalTopN"),
                    input: partial,
                    order_by,
                    limit: top_n.limit,
                    offset: top_n.offset,
                    enable_fixed_rows,
                    stat_info: Some(stat_info.clone()),
                })
            }
        };

        if apply_lazy_materialization {
            self.build_row_fetch_for_lazy_columns(plan, &top_n.lazy_columns, stat_info)
        } else {
            Ok(plan)
        }
    }
}
