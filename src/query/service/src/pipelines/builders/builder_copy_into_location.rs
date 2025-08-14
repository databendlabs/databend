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

use chrono::Duration;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::executor::physical_plans::CopyIntoLocation;
use databend_common_storages_stage::StageSinkTable;
use databend_common_version::DATABEND_SEMVER;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_copy_into_location(&mut self, copy: &CopyIntoLocation) -> Result<()> {
        self.build_pipeline(&copy.input)?;

        // Reorder the result for select clause
        PipelineBuilder::build_result_projection(
            &self.func_ctx,
            copy.input.output_schema()?,
            &copy.project_columns,
            &mut self.main_pipeline,
            false,
        )?;

        // The stage table that copying into
        let to_table = StageSinkTable::create(
            copy.info.clone(),
            copy.input_table_schema.clone(),
            sink_create_by(),
        )?;

        // StageSinkTable needs not to hold the table meta timestamps invariants, just pass a dummy one
        let dummy_table_meta_timestamps = TableMetaTimestamps::new(None, Duration::hours(1));
        PipelineBuilder::build_append2table_with_commit_pipeline(
            self.ctx.clone(),
            &mut self.main_pipeline,
            to_table,
            copy.input_data_schema.clone(),
            None,
            vec![],
            false,
            unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            dummy_table_meta_timestamps,
        )
    }
}

fn sink_create_by() -> String {
    const CREATE_BY_LEN: usize = 24; // "Databend 1.2.333-nightly".len();

    // example:  1.2.333-nightly
    // tags may contain other items like `1.2.680-p2`, we will fill it with `1.2.680-p2.....`
    let mut create_by = format!(
        "Databend {}.{}.{}-{:.<7}",
        DATABEND_SEMVER.major,
        DATABEND_SEMVER.minor,
        DATABEND_SEMVER.patch,
        DATABEND_SEMVER.pre.as_str()
    );

    if create_by.len() != CREATE_BY_LEN {
        create_by = format!("{:.<24}", create_by);
        create_by.truncate(24);
    }
    create_by
}
