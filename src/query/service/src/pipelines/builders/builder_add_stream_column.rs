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

use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::AddStreamColumn;
use databend_common_sql::StreamContext;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::processors::TransformAddStreamColumns;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_add_stream_column(
        &mut self,
        add_stream_column: &AddStreamColumn,
    ) -> Result<()> {
        self.build_pipeline(&add_stream_column.input)?;

        let exprs = add_stream_column
            .exprs
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();

        let operators = vec![
            BlockOperator::Map {
                exprs,
                projections: None,
            },
            BlockOperator::Project {
                projection: add_stream_column.projections.clone(),
            },
        ];

        let stream_context = StreamContext {
            stream_columns: add_stream_column.stream_columns.clone(),
            operators,
            func_ctx: self.ctx.get_function_context()?,
        };

        self.main_pipeline
            .add_transformer(|| TransformAddStreamColumns::new(stream_context.clone()));

        Ok(())
    }
}
