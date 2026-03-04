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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::blocks::TransformCastSchema;
use databend_common_pipeline_transforms::columns::TransformAddComputedColumns;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::DefaultExprBinder;

use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::TransformAsyncFunction;
use crate::pipelines::processors::transforms::TransformResortAddOn;
use crate::sessions::QueryContext;

/// This file implements append to table pipeline builder.
impl PipelineBuilder {
    // Fill missing columns with default or compute expr
    // ** Also reorder the block into table's schema order **
    pub fn fill_and_reorder_columns(
        ctx: Arc<QueryContext>,
        pipeline: &mut Pipeline,
        table: Arc<dyn Table>,
        source_schema: DataSchemaRef,
    ) -> Result<()> {
        let table_default_schema = &table.schema().remove_computed_fields();
        let table_computed_schema = &table.schema().remove_virtual_computed_fields();
        let default_schema: DataSchemaRef = Arc::new(table_default_schema.into());
        let computed_schema: DataSchemaRef = Arc::new(table_computed_schema.into());

        // Fill missing default columns and resort the columns.
        if source_schema != default_schema {
            let mut default_expr_binder =
                DefaultExprBinder::try_new(ctx.clone())?.auto_increment_table_id(table.get_id());
            if let Some((async_funcs, new_default_schema, new_default_schema_no_cast)) =
                default_expr_binder
                    .split_async_default_exprs(source_schema.clone(), default_schema.clone())?
            {
                let sequence_counters =
                    TransformAsyncFunction::create_sequence_counters(async_funcs.len());

                pipeline.try_add_async_transformer(|| {
                    TransformAsyncFunction::new(
                        ctx.clone(),
                        async_funcs.clone(),
                        BTreeMap::new(),
                        sequence_counters.clone(),
                    )
                })?;
                if new_default_schema != new_default_schema_no_cast {
                    pipeline.try_add_transformer(|| {
                        TransformCastSchema::try_new(
                            new_default_schema_no_cast.clone(),
                            new_default_schema.clone(),
                            ctx.get_function_context().unwrap(),
                        )
                    })?;
                }

                pipeline.try_add_transformer(|| {
                    TransformResortAddOn::try_new(
                        ctx.clone(),
                        new_default_schema.clone(),
                        default_schema.clone(),
                        table.clone(),
                    )
                })?;
            } else {
                pipeline.try_add_transformer(|| {
                    TransformResortAddOn::try_new(
                        ctx.clone(),
                        source_schema.clone(),
                        default_schema.clone(),
                        table.clone(),
                    )
                })?;
            }
        }

        // Fill computed columns.
        if default_schema != computed_schema {
            pipeline.try_add_transformer(|| {
                TransformAddComputedColumns::try_new(
                    ctx.clone(),
                    default_schema.clone(),
                    computed_schema.clone(),
                )
            })?;
        }

        Ok(())
    }
}
