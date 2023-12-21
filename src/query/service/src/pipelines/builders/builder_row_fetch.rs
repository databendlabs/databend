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
use databend_common_sql::executor::physical_plans::RowFetch;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_storages_fuse::operations::build_row_fetcher_pipeline;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_row_fetch(&mut self, row_fetch: &RowFetch) -> Result<()> {
        debug_assert!(matches!(&*row_fetch.input, PhysicalPlan::Limit(_)));
        self.build_pipeline(&row_fetch.input)?;
        build_row_fetcher_pipeline(
            self.ctx.clone(),
            &mut self.main_pipeline,
            row_fetch.row_id_col_offset,
            &row_fetch.source,
            row_fetch.cols_to_fetch.clone(),
        )
    }
}
