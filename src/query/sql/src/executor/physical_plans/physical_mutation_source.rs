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

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;

use crate::binder::MutationType;
use crate::executor::cast_expr_to_non_null_boolean;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MutationSource {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub table_index: IndexType,
    pub table_info: TableInfo,
    pub filters: Option<Filters>,
    pub output_schema: DataSchemaRef,
    pub input_type: MutationType,
    pub read_partition_columns: ColumnSet,
    pub truncate_table: bool,

    pub partitions: Partitions,
    pub statistics: PartStatistics,
}

impl MutationSource {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_mutation_source(
        &mut self,
        mutation_source: &crate::plans::MutationSource,
    ) -> Result<PhysicalPlan> {
        let filters = if !mutation_source.predicates.is_empty() {
            Some(create_push_down_filters(
                &self.ctx.get_function_context()?,
                &mutation_source.predicates,
            )?)
        } else {
            None
        };
        let mutation_info = self.mutation_build_info.as_ref().unwrap();

        let metadata = self.metadata.read();
        let mut fields = Vec::with_capacity(mutation_source.columns.len());
        for column_index in mutation_source.columns.iter() {
            let column = metadata.column(*column_index);
            // Ignore virtual computed columns.
            if let Ok(column_id) = mutation_source.schema.index_of(&column.name()) {
                fields.push((column.name(), *column_index, column_id));
            }
        }
        fields.sort_by_key(|(_, _, id)| *id);

        let mut fields = fields
            .into_iter()
            .map(|(name, index, _)| {
                let table_field = mutation_source.schema.field_with_name(&name)?;
                let data_type = DataType::from(table_field.data_type());
                Ok(DataField::new(&index.to_string(), data_type))
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(predicate_index) = mutation_source.predicate_column_index {
            fields.push(DataField::new(
                &predicate_index.to_string(),
                DataType::Boolean,
            ));
        }
        let output_schema = DataSchemaRefExt::create(fields);

        let truncate_table =
            mutation_source.mutation_type == MutationType::Delete && filters.is_none();
        Ok(PhysicalPlan::MutationSource(MutationSource {
            plan_id: 0,
            table_index: mutation_source.table_index,
            output_schema,
            table_info: mutation_info.table_info.clone(),
            filters,
            input_type: mutation_source.mutation_type.clone(),
            read_partition_columns: mutation_source.read_partition_columns.clone(),
            truncate_table,
            partitions: mutation_info.partitions.clone(),
            statistics: mutation_info.statistics.clone(),
        }))
    }
}

/// create push down filters
pub fn create_push_down_filters(
    func_ctx: &FunctionContext,
    predicates: &[ScalarExpr],
) -> Result<Filters> {
    let predicates = predicates
        .iter()
        .map(|p| {
            Ok(p.as_expr()?
                .project_column_ref(|col| col.column_name.clone()))
        })
        .collect::<Result<Vec<_>>>()?;

    let expr = predicates
        .into_iter()
        .try_reduce(|lhs, rhs| {
            check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
        })?
        .unwrap();
    let expr = cast_expr_to_non_null_boolean(expr)?;
    let (filter, _) = ConstantFolder::fold(&expr, func_ctx, &BUILTIN_FUNCTIONS);
    let remote_filter = filter.as_remote_expr();

    // prepare the inverse filter expression
    let remote_inverted_filter =
        check_function(None, "not", &[], &[filter], &BUILTIN_FUNCTIONS)?.as_remote_expr();

    Ok(Filters {
        filter: remote_filter,
        inverted_filter: remote_inverted_filter,
    })
}
