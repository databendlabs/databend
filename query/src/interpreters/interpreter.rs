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

#[async_trait::async_trait]
pub trait Interpreter: Sync + Send {
    fn name(&self) -> &str;

    fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream>;

    /// Do some start work for the interpreter.
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
}

pub type InterpreterPtr = std::sync::Arc<dyn Interpreter>;
