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
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::type_check;
use databend_common_expression::type_check::common_super_type;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::ColumnSet;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::TypeCheck;
use databend_common_sql::binder::wrap_cast;
use databend_common_sql::executor::physical_plans::AggregateFunctionDesc;
use databend_common_sql::executor::physical_plans::AggregateFunctionSignature;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::WindowFuncFrame;
use databend_common_sql::plans::WindowFuncFrameBound;
use databend_common_sql::plans::WindowFuncType;

use super::LagLeadDefault;
use super::LagLeadFunctionDesc;
use super::NthValueFunctionDesc;
use super::NtileFunctionDesc;
use super::WindowFunction;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::Sort;
use crate::physical_plans::WindowPartition;
use crate::physical_plans::WindowPartitionTopN;
use crate::physical_plans::WindowPartitionTopNFunc;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::WindowFormatter;
use crate::physical_plans::format::WindowGroupFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::physical_sort::SortStep;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::builders::SortPipelineBuilder;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::FrameBound;
use crate::pipelines::processors::transforms::SortStrategy;
use crate::pipelines::processors::transforms::TransformWindow;
use crate::pipelines::processors::transforms::TransformWindowPartitionCollect;
use crate::pipelines::processors::transforms::WindowFunctionInfo;
use crate::pipelines::processors::transforms::WindowPartitionExchange;
use crate::pipelines::processors::transforms::WindowPartitionTopNExchange;
use crate::pipelines::processors::transforms::WindowSortDesc;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Window {
    pub meta: PhysicalPlanMeta,
    pub index: Symbol,
    pub input: PhysicalPlan,
    pub func: WindowFunction,
    pub partition_by: Vec<Symbol>,
    pub order_by: Vec<SortDesc>,
    pub window_frame: WindowFuncFrame,
    pub limit: Option<usize>,
    pub top: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WindowSpec {
    pub index: Symbol,
    pub func: WindowFunction,
    pub partition_by: Vec<Symbol>,
    pub order_by: Vec<SortDesc>,
    pub window_frame: WindowFuncFrame,
    pub limit: Option<usize>,
    pub top: Option<usize>,
}

impl Window {
    fn spec(&self) -> WindowSpec {
        WindowSpec {
            index: self.index,
            func: self.func.clone(),
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.clone(),
            window_frame: self.window_frame.clone(),
            limit: self.limit,
            top: self.top,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WindowGroup {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub windows: Vec<WindowSpec>,
}

#[typetag::serde]
impl IPhysicalPlan for WindowGroup {
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
        let mut fields = Vec::with_capacity(input_schema.fields().len() + self.windows.len());
        fields.extend_from_slice(input_schema.fields());
        for window in &self.windows {
            fields.push(DataField::new(
                &window.index.to_string(),
                window.func.data_type()?,
            ));
        }
        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(WindowGroupFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(WindowGroup {
            meta: self.meta.clone(),
            input,
            windows: self.windows.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let mut schema = self.input.output_schema()?;
        let mut previous_order = self.windows.first().map(WindowOrder::from);
        for window in &self.windows {
            let order = WindowOrder::from(window);
            if previous_order
                .as_ref()
                .is_none_or(|previous| !previous.satisfies(&order))
            {
                apply_window_order(builder, &schema, window)?;
            }
            apply_window_transform(builder, &schema, window)?;
            previous_order = Some(order);

            let mut fields = schema.fields().clone();
            fields.push(DataField::new(
                &window.index.to_string(),
                window.func.data_type()?,
            ));
            schema = DataSchemaRefExt::create(fields);
        }
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
struct WindowOrder<'a> {
    partition_by: &'a [Symbol],
    order_by: &'a [SortDesc],
}

impl<'a> From<&'a WindowSpec> for WindowOrder<'a> {
    fn from(window: &'a WindowSpec) -> Self {
        Self {
            partition_by: &window.partition_by,
            order_by: &window.order_by,
        }
    }
}

impl WindowOrder<'_> {
    fn satisfies(&self, required: &WindowOrder) -> bool {
        if required.partition_by.is_empty() {
            return self.partition_by.is_empty() && self.order_by == required.order_by;
        }

        self.partition_by == required.partition_by
            && (required.order_by.is_empty() || self.order_by == required.order_by)
    }
}

#[typetag::serde]
impl IPhysicalPlan for Window {
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
        let mut fields = Vec::with_capacity(input_schema.fields().len() + 1);
        fields.extend_from_slice(input_schema.fields());
        fields.push(DataField::new(
            &self.index.to_string(),
            self.func.data_type()?,
        ));
        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(WindowFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        let partition_by = self
            .partition_by
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let order_by = self
            .order_by
            .iter()
            .map(|x| {
                format!(
                    "{}{}{}",
                    x.display_name,
                    if x.asc { "" } else { " DESC" },
                    if x.nulls_first { " NULLS FIRST" } else { "" },
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        Ok(format!(
            "partition by {}, order by {}",
            partition_by, order_by
        ))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(Window {
            meta: self.meta.clone(),
            index: self.index,
            input,
            func: self.func.clone(),
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.clone(),
            window_frame: self.window_frame.clone(),
            limit: self.limit,
            top: self.top,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;
        let input_schema = self.input.output_schema()?;
        apply_window_transform(builder, &input_schema, &self.spec())
    }
}

fn apply_window_transform(
    builder: &mut PipelineBuilder,
    input_schema: &DataSchema,
    window: &WindowSpec,
) -> Result<()> {
    let partition_by = window
        .partition_by
        .iter()
        .map(|p| input_schema.index_of(&p.to_string()))
        .collect::<Result<Vec<_>>>()?;
    let order_by = window
        .order_by
        .iter()
        .map(|o| {
            let offset = input_schema.index_of(&o.order_by.to_string())?;
            Ok(WindowSortDesc {
                offset,
                asc: o.asc,
                nulls_first: o.nulls_first,
                is_nullable: input_schema.field(offset).is_nullable(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let old_output_len = builder.main_pipeline.output_len();
    if partition_by.is_empty() {
        builder.main_pipeline.try_resize(1)?;
    }
    let func = WindowFunctionInfo::try_create(&window.func, input_schema)?;
    builder.main_pipeline.add_transform(|input, output| {
        let transform = if window.window_frame.units.is_rows() {
            let start_bound = FrameBound::try_from(&window.window_frame.start_bound)?;
            let end_bound = FrameBound::try_from(&window.window_frame.end_bound)?;
            Box::new(TransformWindow::try_create_rows(
                input,
                output,
                func.clone(),
                partition_by.clone(),
                order_by.clone(),
                (start_bound, end_bound),
            )?) as Box<dyn Processor>
        } else {
            let start_bound = FrameBound::try_from(&window.window_frame.start_bound)?;
            let end_bound = FrameBound::try_from(&window.window_frame.end_bound)?;
            Box::new(TransformWindow::try_create_range(
                input,
                output,
                func.clone(),
                partition_by.clone(),
                order_by.clone(),
                (start_bound, end_bound),
            )?) as Box<dyn Processor>
        };
        Ok(ProcessorPtr::create(transform))
    })?;
    if partition_by.is_empty() {
        builder.main_pipeline.try_resize(old_output_len)?;
    }
    Ok(())
}

fn apply_window_order(
    builder: &mut PipelineBuilder,
    input_schema: &DataSchema,
    window: &WindowSpec,
) -> Result<()> {
    let partition_nulls_first = builder.settings.get_nulls_first()(true);
    let sort_desc = window
        .partition_by
        .iter()
        .map(|index| {
            let offset = input_schema.index_of(&index.to_string())?;
            Ok(SortColumnDescription {
                offset,
                asc: true,
                nulls_first: partition_nulls_first,
            })
        })
        .chain(window.order_by.iter().map(|desc| {
            let offset = input_schema.index_of(&desc.order_by.to_string())?;
            Ok(SortColumnDescription {
                offset,
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            })
        }))
        .collect::<Result<Vec<_>>>()?;

    if sort_desc.is_empty() {
        return Ok(());
    }

    if !window.partition_by.is_empty() {
        apply_window_partition(builder, input_schema, window, sort_desc)?;
        return Ok(());
    }

    let enable_fixed_rows = builder.settings.get_enable_fixed_rows_sort()?;
    let sort_builder = SortPipelineBuilder::create(
        builder.ctx.clone(),
        DataSchemaRefExt::create(input_schema.fields().clone()),
        sort_desc.into(),
        None,
        enable_fixed_rows,
    )?;
    let max_threads = builder.settings.get_max_threads()? as usize;
    if builder.main_pipeline.output_len() == 1 || max_threads == 1 {
        builder.main_pipeline.try_resize(max_threads)?;
    }
    sort_builder.build_full_sort_pipeline(&mut builder.main_pipeline, false)
}

fn apply_window_partition(
    builder: &mut PipelineBuilder,
    input_schema: &DataSchema,
    window: &WindowSpec,
    sort_desc: Vec<SortColumnDescription>,
) -> Result<()> {
    let num_processors = builder.main_pipeline.output_len();
    let settings = builder.settings.clone();
    let num_partitions = builder.settings.get_window_num_partitions()?;
    let partition_by = window
        .partition_by
        .iter()
        .map(|index| input_schema.index_of(&index.to_string()))
        .collect::<Result<Vec<_>>>()?;

    if let Some((top, func)) = window_top_n(window) {
        builder.main_pipeline.exchange(
            num_processors,
            WindowPartitionTopNExchange::create(
                partition_by.clone(),
                sort_desc.clone(),
                top,
                func,
                num_partitions as u64,
            ),
        )?;
    } else {
        builder.main_pipeline.exchange(
            num_processors,
            WindowPartitionExchange::create(partition_by.clone(), num_partitions),
        )?;
    }

    let window_spill_settings = MemorySettings::from_window_settings(&builder.ctx)?;
    let plan_schema = DataSchemaRefExt::create(input_schema.fields().clone());
    let processor_id = AtomicUsize::new(0);
    builder.main_pipeline.add_transform(|input, output| {
        let strategy = SortStrategy::try_create(&settings, sort_desc.clone(), plan_schema.clone())?;
        Ok(ProcessorPtr::create(Box::new(
            TransformWindowPartitionCollect::new(
                builder.ctx.clone(),
                input,
                output,
                &settings,
                processor_id.fetch_add(1, atomic::Ordering::AcqRel),
                num_processors,
                num_partitions,
                window_spill_settings.clone(),
                strategy,
            )?,
        )))
    })
}

fn window_top_n(window: &WindowSpec) -> Option<(usize, WindowPartitionTopNFunc)> {
    let top = window.top?;
    if top >= 10000 {
        return None;
    }

    let func = match window.func {
        WindowFunction::RowNumber => WindowPartitionTopNFunc::RowNumber,
        WindowFunction::Rank => WindowPartitionTopNFunc::Rank,
        WindowFunction::DenseRank => WindowPartitionTopNFunc::DenseRank,
        WindowFunction::Aggregate(_)
        | WindowFunction::LagLead(_)
        | WindowFunction::NthValue(_)
        | WindowFunction::Ntile(_)
        | WindowFunction::PercentRank
        | WindowFunction::CumeDist => return None,
    };

    Some((top, func))
}

impl PhysicalPlanBuilder {
    pub async fn build_window_group(
        &mut self,
        s_expr: &SExpr,
        window_group: &databend_common_sql::plans::WindowGroup,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        for window in &window_group.windows {
            required.remove(&window.index);
        }
        for item in &window_group.scalar_items {
            required.extend(item.scalar.used_columns());
            required.insert(item.index);
        }
        for window in &window_group.windows {
            for item in &window.arguments {
                required.extend(item.scalar.used_columns());
                required.insert(item.index);
            }
            for item in &window.partition_by {
                required.extend(item.scalar.used_columns());
                required.insert(item.index);
            }
            for item in &window.order_by {
                required.extend(item.order_by_item.scalar.used_columns());
                required.insert(item.order_by_item.index);
            }
        }

        let child = s_expr.child(0)?;
        let input = self.build(child, required.clone()).await?;
        let input = if window_group.scalar_items.is_empty() {
            input
        } else {
            let mut projections = required.iter().copied().collect::<Vec<_>>();
            for item in &window_group.scalar_items {
                projections.push(item.index);
            }
            projections.sort_unstable();
            projections.dedup();
            let eval_scalar = databend_common_sql::plans::EvalScalar {
                items: window_group.scalar_items.clone(),
            };
            self.create_eval_scalar(&eval_scalar, projections, input, stat_info.clone())?
        };

        let mut window_specs = Vec::with_capacity(window_group.windows.len());
        let mut schema = input.output_schema()?;
        for window in &window_group.windows {
            let spec = self.create_window_spec(&schema, window)?;
            let mut fields = schema.fields().clone();
            fields.push(DataField::new(
                &spec.index.to_string(),
                spec.func.data_type()?,
            ));
            schema = DataSchemaRefExt::create(fields);
            window_specs.push(spec);
        }

        if window_specs.len() == 1 {
            let spec = window_specs.remove(0);
            let settings = self.ctx.get_settings();
            let enable_fixed_rows = settings.get_enable_fixed_rows_sort()?;
            let input = apply_window_sort_plan(input, &spec, enable_fixed_rows, stat_info)?;
            return Ok(PhysicalPlan::new(Window {
                input,
                index: spec.index,
                func: spec.func,
                partition_by: spec.partition_by,
                order_by: spec.order_by,
                window_frame: spec.window_frame,
                limit: spec.limit,
                top: spec.top,
                meta: PhysicalPlanMeta::new("Window"),
            }));
        }

        let settings = self.ctx.get_settings();
        let enable_fixed_rows = settings.get_enable_fixed_rows_sort()?;
        let input = apply_window_sort_plan(input, &window_specs[0], enable_fixed_rows, stat_info)?;

        Ok(PhysicalPlan::new(WindowGroup {
            meta: PhysicalPlanMeta::new("WindowGroup"),
            input,
            windows: window_specs,
        }))
    }

    pub async fn build_window(
        &mut self,
        s_expr: &SExpr,
        window: &databend_common_sql::plans::Window,
        mut required: ColumnSet,
        _stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. DO NOT Prune unused Columns cause window may not in required, eg:
        // select s1.a from ( select t1.a as a, dense_rank() over(order by t1.a desc) as rk
        // from (select 'a1' as a) t1 ) s1
        // left join ( select dense_rank() over(order by t1.a desc) as rk
        // from (select 'a2' as a) t1 )s2 on s1.rk=s2.rk;

        // The scalar items in window function is not replaced yet.
        // The will be replaced in physical plan builder.
        window.arguments.iter().for_each(|item| {
            required.extend(item.scalar.used_columns());
            required.insert(item.index);
        });
        window.partition_by.iter().for_each(|item| {
            required.extend(item.scalar.used_columns());
            required.insert(item.index);
        });
        window.order_by.iter().for_each(|item| {
            required.extend(item.order_by_item.scalar.used_columns());
            required.insert(item.order_by_item.index);
        });

        // 2. Build physical plan.
        let input = self.build(s_expr.child(0)?, required).await?;
        let input_schema = input.output_schema()?;
        let spec = self.create_window_spec(&input_schema, window)?;

        Ok(PhysicalPlan::new(Window {
            input,
            index: spec.index,
            func: spec.func,
            partition_by: spec.partition_by,
            order_by: spec.order_by,
            window_frame: spec.window_frame,
            limit: spec.limit,
            top: spec.top,
            meta: PhysicalPlanMeta::new("Window"),
        }))
    }

    fn create_window_spec(
        &self,
        input_schema: &DataSchema,
        window: &databend_common_sql::plans::Window,
    ) -> Result<WindowSpec> {
        let mut w = window.clone();

        if w.frame.units.is_range() && w.order_by.len() == 1 {
            let order_by = &mut w.order_by[0].order_by_item.scalar;

            let mut start = match &mut w.frame.start_bound {
                WindowFuncFrameBound::Preceding(scalar)
                | WindowFuncFrameBound::Following(scalar) => scalar.as_mut(),
                _ => None,
            };
            let mut end = match &mut w.frame.end_bound {
                WindowFuncFrameBound::Preceding(scalar)
                | WindowFuncFrameBound::Following(scalar) => scalar.as_mut(),
                _ => None,
            };

            let mut common_ty = order_by.type_check(input_schema)?.data_type().clone();
            if common_ty.remove_nullable().is_timestamp() {
                for scalar in start.iter_mut().chain(end.iter_mut()) {
                    let scalar_ty = scalar.as_ref().infer_data_type();
                    if !scalar_ty.is_interval() {
                        return Err(ErrorCode::IllegalDataType(format!(
                            "when the type of the order by in window func is Timestamp, Preceding and Following can only be INTERVAL types, but get {}",
                            scalar_ty
                        )));
                    }
                }
            } else if common_ty.remove_nullable().is_date() {
                let mut last_ty = None;
                for scalar in start.iter_mut().chain(end.iter_mut()) {
                    let scalar_ty = scalar.as_ref().infer_data_type();
                    if !scalar_ty.is_interval() && !scalar_ty.is_unsigned_numeric() {
                        return Err(ErrorCode::IllegalDataType(format!(
                            "when the type of the order by in window func is Date, Preceding and Following can only be INTERVAL or Unsigned Integer types, but get {}",
                            scalar_ty
                        )));
                    }
                    if last_ty.as_ref().is_none_or(|ty| ty == &scalar_ty) {
                        last_ty = Some(scalar_ty);
                        continue;
                    }
                    return Err(ErrorCode::IllegalDataType(format!(
                        "when the type of the order by in window func is Date, Preceding and Following can only be of the same type, but get {} and {}",
                        last_ty.unwrap(),
                        scalar_ty
                    )));
                }
            } else {
                for scalar in start.iter_mut().chain(end.iter_mut()) {
                    let ty = scalar.as_ref().infer_data_type();
                    common_ty = common_super_type(
                        common_ty.clone(),
                        ty.clone(),
                        &BUILTIN_FUNCTIONS.default_cast_rules,
                    )
                    .ok_or_else(|| {
                        ErrorCode::IllegalDataType(format!(
                            "Cannot find common type for {:?} and {:?}",
                            &common_ty, &ty
                        ))
                    })?;
                }
                *order_by = wrap_cast(order_by, &common_ty);

                for scalar in start.iter_mut().chain(end.iter_mut()) {
                    let raw_expr = RawExpr::<usize>::Cast {
                        span: w.span,
                        is_try: false,
                        expr: Box::new(RawExpr::Constant {
                            span: w.span,
                            scalar: scalar.clone(),
                            data_type: None,
                        }),
                        dest_type: common_ty.clone(),
                    };
                    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
                    let (expr, _) = ConstantFolder::fold(
                        &expr,
                        &FunctionContext::default(),
                        &BUILTIN_FUNCTIONS,
                    );
                    if let Expr::Constant(Constant {
                        scalar: new_scalar, ..
                    }) = expr
                    {
                        if new_scalar.is_positive() {
                            **scalar = new_scalar;
                            continue;
                        }
                    }
                    return Err(ErrorCode::SemanticError(
                        "Only positive numbers are allowed in RANGE offset".to_string(),
                    )
                    .set_span(w.span));
                }
            }
        }

        let settings = self.ctx.get_settings();
        let default_nulls_first = settings.get_nulls_first();
        let order_by = w
            .order_by
            .iter()
            .map(|v| {
                let asc = v.asc.unwrap_or(true);
                SortDesc {
                    asc,
                    nulls_first: v.nulls_first.unwrap_or_else(|| default_nulls_first(asc)),
                    order_by: v.order_by_item.index,
                    display_name: self.metadata.read().column(v.order_by_item.index).name(),
                }
            })
            .collect::<Vec<_>>();
        let partition_by = w.partition_by.iter().map(|v| v.index).collect::<Vec<_>>();

        let func = self.create_window_function(&w)?;

        Ok(WindowSpec {
            index: w.index,
            func,
            partition_by,
            order_by,
            window_frame: w.frame.clone(),
            limit: w.limit,
            top: w.top,
        })
    }

    fn create_window_function(
        &self,
        w: &databend_common_sql::plans::Window,
    ) -> Result<WindowFunction> {
        match &w.function {
            WindowFuncType::Aggregate(agg) => {
                Ok(WindowFunction::Aggregate(AggregateFunctionDesc {
                    sig: AggregateFunctionSignature {
                        name: agg.func_name.clone(),
                        udaf: None,
                        return_type: *agg.return_type.clone(),
                        args: agg
                            .args
                            .iter()
                            .map(|s| s.data_type())
                            .collect::<Result<_>>()?,
                        params: agg.params.clone(),
                        sort_descs: agg
                            .sort_descs
                            .iter()
                            .map(|d| d.try_into())
                            .collect::<Result<_>>()?,
                    },
                    output_column: w.index,
                    arg_indices: agg
                        .args
                        .iter()
                        .map(|arg| {
                            if let ScalarExpr::BoundColumnRef(col) = arg {
                                Ok(col.column.index)
                            } else {
                                Err(ErrorCode::Internal(
                                    "Aggregate function argument must be a BoundColumnRef"
                                        .to_string(),
                                ))
                            }
                        })
                        .collect::<Result<_>>()?,
                    sort_desc_indices: agg
                        .sort_descs
                        .iter()
                        .map(|desc| {
                            if let ScalarExpr::BoundColumnRef(col) = &desc.expr {
                                Ok(col.column.index)
                            } else {
                                Err(ErrorCode::Internal(
                                    "Aggregate function sort description must be a BoundColumnRef"
                                        .to_string(),
                                ))
                            }
                        })
                        .collect::<Result<_>>()?,
                    display: ScalarExpr::AggregateFunction(agg.clone())
                        .as_expr()?
                        .sql_display(),
                }))
            }
            WindowFuncType::LagLead(lag_lead) => {
                let new_default = match &lag_lead.default {
                    None => LagLeadDefault::Null,
                    Some(d) => match d {
                        box ScalarExpr::BoundColumnRef(col) => {
                            LagLeadDefault::Index(col.column.index)
                        }
                        _ => unreachable!(),
                    },
                };
                Ok(WindowFunction::LagLead(LagLeadFunctionDesc {
                    is_lag: lag_lead.is_lag,
                    offset: lag_lead.offset,
                    return_type: *lag_lead.return_type.clone(),
                    arg: if let ScalarExpr::BoundColumnRef(col) = *lag_lead.arg.clone() {
                        Ok(col.column.index)
                    } else {
                        Err(ErrorCode::Internal(
                            "Window's lag function argument must be a BoundColumnRef".to_string(),
                        ))
                    }?,
                    default: new_default,
                }))
            }
            WindowFuncType::NthValue(func) => Ok(WindowFunction::NthValue(NthValueFunctionDesc {
                n: func.n,
                return_type: *func.return_type.clone(),
                arg: if let ScalarExpr::BoundColumnRef(col) = &*func.arg {
                    Ok(col.column.index)
                } else {
                    Err(ErrorCode::Internal(
                        "Window's nth_value function argument must be a BoundColumnRef".to_string(),
                    ))
                }?,
                ignore_null: func.ignore_null,
            })),
            WindowFuncType::Ntile(func) => Ok(WindowFunction::Ntile(NtileFunctionDesc {
                n: func.n,
                return_type: *func.return_type.clone(),
            })),
            WindowFuncType::RowNumber => Ok(WindowFunction::RowNumber),
            WindowFuncType::Rank => Ok(WindowFunction::Rank),
            WindowFuncType::DenseRank => Ok(WindowFunction::DenseRank),
            WindowFuncType::PercentRank => Ok(WindowFunction::PercentRank),
            WindowFuncType::CumeDist => Ok(WindowFunction::CumeDist),
        }
    }
}

fn apply_window_sort_plan(
    input: PhysicalPlan,
    window: &WindowSpec,
    enable_fixed_rows: bool,
    stat_info: PlanStatsInfo,
) -> Result<PhysicalPlan> {
    let mut order_by = Vec::with_capacity(window.partition_by.len() + window.order_by.len());
    for index in &window.partition_by {
        order_by.push(SortDesc {
            asc: true,
            nulls_first: false,
            order_by: *index,
            display_name: index.to_string(),
        });
    }
    order_by.extend(window.order_by.clone());

    if order_by.is_empty() {
        return Ok(input);
    }

    if !window.partition_by.is_empty() {
        return Ok(PhysicalPlan::new(WindowPartition {
            meta: PhysicalPlanMeta::new("WindowPartition"),
            input,
            partition_by: window.partition_by.clone(),
            order_by,
            top_n: window_top_n(window).map(|(top, func)| WindowPartitionTopN { func, top }),
            stat_info: Some(stat_info),
        }));
    }

    Ok(PhysicalPlan::new(Sort {
        meta: PhysicalPlanMeta::new("Sort"),
        input,
        order_by,
        limit: None,
        step: SortStep::Single,
        pre_projection: None,
        broadcast_id: None,
        enable_fixed_rows,
        stat_info: Some(stat_info),
    }))
}
