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

use common_ast::ast::ExplainKind;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;
use crate::sql::exec::PipelineBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;
use crate::sql::BindContext;
use crate::sql::MetadataRef;

pub struct CopyInterpreterV2 {
    ctx: Arc<QueryContext>,
    plan: CopyPlanV2,
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreterV2 {
    fn name(&self) -> &str {
        "CopyInterpreterV2"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        todo!()
    }
}
