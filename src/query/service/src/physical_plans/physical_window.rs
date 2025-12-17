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
use std::fmt::Display;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::type_check;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeCheck;
use databend_common_sql::binder::wrap_cast;
use databend_common_sql::executor::physical_plans::AggregateFunctionDesc;
use databend_common_sql::executor::physical_plans::AggregateFunctionSignature;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::WindowFuncFrame;
use databend_common_sql::plans::WindowFuncFrameBound;
use databend_common_sql::plans::WindowFuncType;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::WindowFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::FrameBound;
use crate::pipelines::processors::transforms::TransformWindow;
use crate::pipelines::processors::transforms::WindowFunctionInfo;
use crate::pipelines::processors::transforms::WindowSortDesc;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Window {
    pub meta: PhysicalPlanMeta,
    pub index: IndexType,
    pub input: PhysicalPlan,
    pub func: WindowFunction,
    pub partition_by: Vec<IndexType>,
    pub order_by: Vec<SortDesc>,
    pub window_frame: WindowFuncFrame,
    pub limit: Option<usize>,
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
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let input_schema = self.input.output_schema()?;
        let partition_by = self
            .partition_by
            .iter()
            .map(|p| {
                let offset = input_schema.index_of(&p.to_string())?;
                Ok(offset)
            })
            .collect::<Result<Vec<_>>>()?;
        let order_by = self
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
        // `TransformWindow` is a pipeline breaker.
        if partition_by.is_empty() {
            builder.main_pipeline.try_resize(1)?;
        }
        let func = WindowFunctionInfo::try_create(&self.func, &input_schema)?;
        // Window
        builder.main_pipeline.add_transform(|input, output| {
            // The transform can only be created here, because it cannot be cloned.

            let transform = if self.window_frame.units.is_rows() {
                let start_bound = FrameBound::try_from(&self.window_frame.start_bound)?;
                let end_bound = FrameBound::try_from(&self.window_frame.end_bound)?;
                Box::new(TransformWindow::try_create_rows(
                    input,
                    output,
                    func.clone(),
                    partition_by.clone(),
                    order_by.clone(),
                    (start_bound, end_bound),
                )?) as Box<dyn Processor>
            } else {
                if order_by.len() == 1 {
                    let start_bound = FrameBound::try_from(&self.window_frame.start_bound)?;
                    let end_bound = FrameBound::try_from(&self.window_frame.end_bound)?;
                    return Ok(ProcessorPtr::create(
                        Box::new(TransformWindow::try_create_range(
                            input,
                            output,
                            func.clone(),
                            partition_by.clone(),
                            order_by.clone(),
                            (start_bound, end_bound),
                        )?) as Box<dyn Processor>,
                    ));
                }

                // There is no offset in the RANGE frame. (just CURRENT ROW or UNBOUNDED)
                // So we can use any number type to create the transform.
                let start_bound = FrameBound::try_from(&self.window_frame.start_bound)?;
                let end_bound = FrameBound::try_from(&self.window_frame.end_bound)?;
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
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum WindowFunction {
    Aggregate(AggregateFunctionDesc),
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    LagLead(LagLeadFunctionDesc),
    NthValue(NthValueFunctionDesc),
    Ntile(NtileFunctionDesc),
    CumeDist,
}

impl WindowFunction {
    fn data_type(&self) -> Result<DataType> {
        let return_type = match self {
            WindowFunction::Aggregate(agg) => agg.sig.return_type.clone(),
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                DataType::Number(NumberDataType::UInt64)
            }
            WindowFunction::PercentRank | WindowFunction::CumeDist => {
                DataType::Number(NumberDataType::Float64)
            }
            WindowFunction::LagLead(f) => f.return_type.clone(),
            WindowFunction::NthValue(f) => f.return_type.clone(),
            WindowFunction::Ntile(f) => f.return_type.clone(),
        };
        Ok(return_type)
    }
}

impl Display for WindowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            WindowFunction::Aggregate(agg) => write!(f, "{}", agg.sig.name),
            WindowFunction::RowNumber => write!(f, "row_number"),
            WindowFunction::Rank => write!(f, "rank"),
            WindowFunction::DenseRank => write!(f, "dense_rank"),
            WindowFunction::PercentRank => write!(f, "percent_rank"),
            WindowFunction::LagLead(lag_lead) if lag_lead.is_lag => write!(f, "lag"),
            WindowFunction::LagLead(_) => write!(f, "lead"),
            WindowFunction::NthValue(_) => write!(f, "nth_value"),
            WindowFunction::Ntile(_) => write!(f, "ntile"),
            WindowFunction::CumeDist => write!(f, "cume_dist"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LagLeadDefault {
    Null,
    Index(IndexType),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LagLeadFunctionDesc {
    pub is_lag: bool,
    pub offset: u64,
    pub arg: usize,
    pub return_type: DataType,
    pub default: LagLeadDefault,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NthValueFunctionDesc {
    pub n: Option<u64>,
    pub arg: usize,
    pub return_type: DataType,
    pub ignore_null: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NtileFunctionDesc {
    pub n: u64,
    pub return_type: DataType,
}

impl PhysicalPlanBuilder {
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
        let mut w = window.clone();

        let input_schema = input.output_schema()?;

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

            let mut common_ty = order_by.type_check(&*input_schema)?.data_type().clone();
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

        let order_by_items = w
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
        let partition_items = w.partition_by.iter().map(|v| v.index).collect::<Vec<_>>();

        let func = match &w.function {
            WindowFuncType::Aggregate(agg) => WindowFunction::Aggregate(AggregateFunctionDesc {
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
                                "Aggregate function argument must be a BoundColumnRef".to_string(),
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
            }),
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
                WindowFunction::LagLead(LagLeadFunctionDesc {
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
                })
            }

            WindowFuncType::NthValue(func) => WindowFunction::NthValue(NthValueFunctionDesc {
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
            }),
            WindowFuncType::Ntile(func) => WindowFunction::Ntile(NtileFunctionDesc {
                n: func.n,
                return_type: *func.return_type.clone(),
            }),
            WindowFuncType::RowNumber => WindowFunction::RowNumber,
            WindowFuncType::Rank => WindowFunction::Rank,
            WindowFuncType::DenseRank => WindowFunction::DenseRank,
            WindowFuncType::PercentRank => WindowFunction::PercentRank,
            WindowFuncType::CumeDist => WindowFunction::CumeDist,
        };

        Ok(PhysicalPlan::new(Window {
            input,
            index: w.index,
            func,
            partition_by: partition_items,
            order_by: order_by_items,
            window_frame: w.frame.clone(),
            limit: w.limit,
            meta: PhysicalPlanMeta::new("Window"),
        }))
    }
}
