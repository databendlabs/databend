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

use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;

use crate::pipelines::PipelineBuildResult;
use crate::pipelines::SourcePipeBuilder;

#[async_trait::async_trait]
/// Interpreter is a trait for different PlanNode
/// Each type of planNode has its own corresponding interpreter
pub trait Interpreter: Sync + Send {
    /// Return the name of Interpreter, such as "CreateDatabaseInterpreter"
    fn name(&self) -> &str;

    /// Return the schema of Interpreter
    fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }

    /// The core of the databend processor which will execute the logical plan and get the DataBlock
    async fn execute(&self) -> Result<SendableDataBlockStream>;

    /// Create the new pipeline for databend's new execution model
    /// Currently databend is developing a new execution model with a hybrid pull-based & push-based strategy
    /// The method now only is implemented by SelectInterpreter
    async fn create_new_pipeline(&self) -> Result<PipelineBuildResult> {
        Err(ErrorCode::UnImplement(format!(
            "UnImplement create_new_pipeline method for {:?}",
            self.name()
        )))
    }

    /// Do some start work for the interpreter
    /// Such as query counts, query start time and etc
    async fn start(&self) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "UnImplement start method for {:?}",
            self.name()
        )))
    }

    /// Do some finish work for the interpreter.
    /// Such as get the metrics and write to query log etc.
    async fn finish(&self) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "UnImplement finish method for {:?}",
            self.name()
        )))
    }

    fn set_source_pipe_builder(&self, _builder: Option<SourcePipeBuilder>) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "UnImplement set_source_pipe_builder method for {:?}",
            self.name()
        )))
    }
}

pub type InterpreterPtr = std::sync::Arc<dyn Interpreter>;
