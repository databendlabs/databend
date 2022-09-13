// Copyright 2022 Datafuse Labs.
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
use common_catalog::table::Table;
use common_pipeline_core::Pipeline;

pub struct PipelineBuildResult {
    pub main_pipeline: Pipeline,
    // Containing some sub queries pipelines, must be complete pipeline
    pub sources_pipelines: Vec<Pipeline>,
}

impl PipelineBuildResult {
    pub fn create() -> PipelineBuildResult {
        PipelineBuildResult {
            main_pipeline: Pipeline::create(),
            sources_pipelines: vec![],
        }
    }

    pub fn set_max_threads(&mut self, max_threads: usize) {
        self.main_pipeline.set_max_threads(max_threads);

        for source_pipeline in &mut self.sources_pipelines {
            source_pipeline.set_max_threads(max_threads);
        }
    }
}
