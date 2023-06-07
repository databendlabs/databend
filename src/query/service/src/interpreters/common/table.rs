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

use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_meta_app::schema::UpsertTableCopiedFileReq;

use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub fn append2table(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    build_res: &mut PipelineBuildResult,
    copied_files: Option<UpsertTableCopiedFileReq>,
    overwrite: bool,
    append_mode: AppendMode,
) -> Result<()> {
    let table_data_schema = Arc::new(table.schema().into());

    if source_schema != table_data_schema {
        build_res
            .main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformResortAddOn::try_create(
                    ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    source_schema.clone(),
                    table.clone(),
                )
            })?;
    }

    table.append_data(ctx.clone(), &mut build_res.main_pipeline, append_mode)?;

    table.commit_insertion(ctx, &mut build_res.main_pipeline, copied_files, overwrite)?;

    Ok(())
}
