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

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;

use crate::pipelines::processors::transforms::TransformAddComputedColumns;
use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::PipelineBuilder;
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
            pipeline.try_add_transformer(|| {
                TransformResortAddOn::try_new(
                    ctx.clone(),
                    source_schema.clone(),
                    default_schema.clone(),
                    table.clone(),
                )
            })?;
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
