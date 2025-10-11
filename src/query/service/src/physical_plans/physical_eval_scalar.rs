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
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::optimizer::ir::Matcher;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Filter;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::ProjectSet;
use databend_common_sql::plans::RelOp;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::plans::ScalarItem;
use databend_common_sql::plans::Visitor;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::TypeCheck;
use itertools::Itertools;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::EvalScalarFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::physical_plan_builder::PhysicalPlanBuilder;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EvalScalar {
    meta: PhysicalPlanMeta,
    pub projections: ColumnSet,
    pub input: PhysicalPlan,
    pub exprs: Vec<(RemoteExpr, IndexType)>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for EvalScalar {
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
        if self.exprs.is_empty() {
            return self.input.output_schema();
        }
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(self.projections.len());
        for (i, field) in input_schema.fields().iter().enumerate() {
            if self.projections.contains(&i) {
                fields.push(field.clone());
            }
        }
        let input_column_nums = input_schema.num_fields();
        for (i, (expr, index)) in self.exprs.iter().enumerate() {
            let i = i + input_column_nums;
            if !self.projections.contains(&i) {
                continue;
            }
            let name = index.to_string();
            let data_type = expr.as_expr(&BUILTIN_FUNCTIONS).data_type().clone();
            fields.push(DataField::new(&name, data_type));
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
        Ok(EvalScalarFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn display_in_profile(&self) -> bool {
        !self.exprs.is_empty()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .exprs
            .iter()
            .map(|(x, _)| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .join(", "))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::from([(
            String::from("List of Expressions"),
            self.exprs
                .iter()
                .map(|(x, _)| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .collect(),
        )]))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(EvalScalar {
            meta: self.meta.clone(),
            projections: self.projections.clone(),
            input,
            exprs: self.exprs.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let input_schema = self.input.output_schema()?;
        let exprs = self
            .exprs
            .iter()
            .map(|(scalar, _)| scalar.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();

        if exprs.is_empty() {
            return Ok(());
        }

        let op = BlockOperator::Map {
            exprs,
            projections: Some(self.projections.clone()),
        };

        let num_input_columns = input_schema.num_fields();

        builder.main_pipeline.add_transformer(|| {
            CompoundBlockOperator::new(
                vec![op.clone()],
                builder.func_ctx.clone(),
                num_input_columns,
            )
        });

        Ok(())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_eval_scalar(
        &mut self,
        s_expr: &SExpr,
        eval_scalar: &databend_common_sql::plans::EvalScalar,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let column_projections = required.clone();
        let mut used = vec![];
        // Only keep columns needed by parent plan.
        for s in eval_scalar.items.iter() {
            if !required.contains(&s.index) {
                continue;
            }
            used.push(s.clone());
        }
        let mut child_required = self.derive_child_required_columns(s_expr, &required)?;
        debug_assert_eq!(child_required.len(), s_expr.arity());
        let child_required = child_required.remove(0);

        // 2. Build physical plan.
        if used.is_empty() {
            self.build(s_expr.child(0)?, child_required).await
        } else {
            let child = s_expr.child(0)?;
            let input = if let Some(new_child) = self.try_eliminate_flatten_columns(&used, child)? {
                self.build(&new_child, child_required.clone()).await?
            } else {
                self.build(child, child_required).await?
            };

            let column_projections: HashSet<usize> = column_projections
                .union(self.metadata.read().get_retained_column())
                .cloned()
                .collect();
            let column_projections = column_projections.clone().into_iter().collect::<Vec<_>>();
            let eval_scalar = databend_common_sql::plans::EvalScalar { items: used };
            self.create_eval_scalar(&eval_scalar, column_projections, input, stat_info)
        }
    }

    pub fn create_eval_scalar(
        &mut self,
        eval_scalar: &databend_common_sql::plans::EvalScalar,
        column_projections: Vec<IndexType>,
        input: PhysicalPlan,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let input_schema = input.output_schema()?;
        let exprs = eval_scalar
            .items
            .iter()
            .map(|item| {
                let expr = item
                    .scalar
                    .type_check(input_schema.as_ref())?
                    .project_column_ref(|index| input_schema.index_of(&index.to_string()))?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                Ok((expr.as_remote_expr(), item.index))
            })
            .collect::<Result<Vec<_>>>()?;

        let exprs = exprs
            .into_iter()
            .filter(|(scalar, idx)| {
                if let RemoteExpr::ColumnRef { id, .. } = scalar {
                    return idx.to_string() != input_schema.field(*id).name().as_str();
                }
                true
            })
            .collect::<Vec<_>>();

        let mut projections = ColumnSet::new();
        for column in column_projections.iter() {
            if let Some((index, _)) = input_schema.column_with_name(&column.to_string()) {
                projections.insert(index);
            }
        }
        let input_column_nums = input_schema.num_fields();
        for (index, (_, idx)) in exprs.iter().enumerate() {
            if column_projections.contains(idx) {
                projections.insert(index + input_column_nums);
            }
        }
        Ok(PhysicalPlan::new(EvalScalar {
            input,
            exprs,
            projections,
            stat_info: Some(stat_info),
            meta: PhysicalPlanMeta::new("EvalScalar"),
        }))
    }

    fn try_eliminate_flatten_columns(
        &mut self,
        scalar_items: &Vec<ScalarItem>,
        s_expr: &SExpr,
    ) -> Result<Option<SExpr>> {
        // (1) ProjectSet
        //      \
        //       *
        //
        // (2) Filter
        //      \
        //       ProjectSet
        //        \
        //         *
        let matchers = vec![
            Matcher::MatchOp {
                op_type: RelOp::ProjectSet,
                children: vec![Matcher::Leaf],
            },
            Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::ProjectSet,
                    children: vec![Matcher::Leaf],
                }],
            },
        ];

        let mut matched = false;
        for matcher in matchers {
            if matcher.matches(s_expr) {
                matched = true;
                break;
            }
        }
        if !matched {
            return Ok(None);
        }

        if let RelOperator::Filter(filter) = s_expr.plan() {
            let child = s_expr.child(0)?;
            let project_set: ProjectSet = child.plan().clone().try_into()?;
            let Some(new_project_set) =
                self.eliminate_flatten_columns(scalar_items, Some(filter), &project_set)
            else {
                return Ok(None);
            };
            let mut new_child = child.clone();
            new_child.plan = Arc::new(new_project_set.into());
            let new_filter =
                SExpr::create_unary(Arc::new(s_expr.plan().clone()), Arc::new(new_child));
            Ok(Some(new_filter))
        } else {
            let project_set: ProjectSet = s_expr.plan().clone().try_into()?;
            let Some(new_project_set) =
                self.eliminate_flatten_columns(scalar_items, None, &project_set)
            else {
                return Ok(None);
            };
            let mut new_expr = s_expr.clone();
            new_expr.plan = Arc::new(new_project_set.into());
            Ok(Some(new_expr))
        }
    }

    // The flatten function returns a tuple, which contains 6 columns.
    // Only keep columns required by parent plan, other columns can be eliminated
    // to reduce the memory usage.
    fn eliminate_flatten_columns(
        &mut self,
        scalar_items: &Vec<ScalarItem>,
        filter: Option<&Filter>,
        project_set: &ProjectSet,
    ) -> Option<ProjectSet> {
        let mut has_flatten = false;
        let mut project_set = project_set.clone();
        for srf_item in &mut project_set.srfs {
            if let ScalarExpr::FunctionCall(srf_func) = &srf_item.scalar {
                if srf_func.func_name == "flatten" {
                    has_flatten = true;
                    let mut visitor = FlattenColumnsVisitor {
                        params: BTreeSet::new(),
                        column_index: srf_item.index,
                    };
                    // Collect columns required by the parent plan in params.
                    for item in scalar_items {
                        visitor.visit(&item.scalar).unwrap();
                    }
                    if let Some(filter) = filter {
                        for pred in &filter.predicates {
                            visitor.visit(pred).unwrap();
                        }
                    }

                    srf_item.scalar = ScalarExpr::FunctionCall(FunctionCall {
                        span: srf_func.span,
                        func_name: srf_func.func_name.clone(),
                        params: visitor.params.into_iter().collect::<Vec<_>>(),
                        arguments: srf_func.arguments.clone(),
                    });
                }
            }
        }
        if has_flatten {
            Some(project_set)
        } else {
            None
        }
    }
}

struct FlattenColumnsVisitor {
    params: BTreeSet<Scalar>,
    column_index: IndexType,
}

impl<'a> Visitor<'a> for FlattenColumnsVisitor {
    // Collect the params in get function which is used to extract the inner column of flatten function.
    fn visit_function_call(&mut self, func: &'a FunctionCall) -> Result<()> {
        if func.func_name == "get" && !func.arguments.is_empty() {
            if let ScalarExpr::BoundColumnRef(column_ref) = &func.arguments[0] {
                if column_ref.column.index == self.column_index {
                    self.params.insert(func.params[0].clone());
                    return Ok(());
                }
            }
        }
        for expr in &func.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }
}
