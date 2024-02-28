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

use databend_common_base::runtime::profile::Profile;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::pipelines::executor::QueriesPipelineExecutor;
use crate::pipelines::executor::QueryPipelineExecutor;

pub enum PipelineExecutor {
    QueryPipelineExecutor(Arc<QueryPipelineExecutor>),
    QueriesPipelineExecutor(Arc<QueriesPipelineExecutor>),
}

impl PipelineExecutor {
    pub fn execute(&self) -> Result<()> {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.execute(),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.execute(),
        }
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.finish(cause),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.finish(cause),
        }
    }

    pub fn is_finished(&self) -> bool {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.is_finished(),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.is_finished(),
        }
    }

    pub fn format_graph_nodes(&self) -> String {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.format_graph_nodes(),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.format_graph_nodes(),
        }
    }

    pub fn get_profiles(&self) -> Vec<Arc<Profile>> {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.get_profiles(),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.get_profiles(),
        }
    }
}
