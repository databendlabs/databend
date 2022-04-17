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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::EmptyPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

// EmptyInterpreter is a Empty interpreter to execute nothing.
// Such as the query '/*!40101*/', it makes the front-end (e.g. MySQL handler) no need check the EmptyPlan or not.
pub struct EmptyInterpreter {}

impl EmptyInterpreter {
    pub fn try_create(_ctx: Arc<QueryContext>, _plan: EmptyPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(EmptyInterpreter {}))
    }
}

#[async_trait::async_trait]
impl Interpreter for EmptyInterpreter {
    fn name(&self) -> &str {
        "EmptyInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(DataBlockStream::create(
            Arc::new(DataSchema::empty()),
            None,
            vec![],
        )))
    }
}
