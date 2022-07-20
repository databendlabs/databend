// Copyright 2021 Datafuse Labs.
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

pub mod executor;
pub mod processors;

use common_pipeline::pipe;
use common_pipeline::pipeline;

mod pipeline_builder;
mod pipeline_build_res;

pub use pipe::Pipe;
pub use pipe::SinkPipeBuilder;
pub use pipe::SourcePipeBuilder;
pub use pipe::TransformPipeBuilder;
pub use pipeline::Pipeline;
pub use pipeline_builder::QueryPipelineBuilder;
pub use pipeline_build_res::PipelineBuildResult;
