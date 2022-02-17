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

use std::sync::Arc;

use common_exception::Result;
use common_planners::CopyPlan;
use common_streams::DataBlockStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceFactory;
use common_streams::SourceParams;
use common_streams::SourceStream;
use futures::io::BufReader;
use futures::TryStreamExt;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_until;
use nom::IResult;
use opendal::credential::Credential;
use opendal::readers::SeekableReader;
use opendal::services::s3;
use opendal::Operator as DalOperator;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct CopyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyPlan,
}

impl CopyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CopyInterpreter { ctx, plan }))
    }

    async fn get_dal(&self) -> Result<DalOperator> {
        todo!()
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreter {
    fn name(&self) -> &str {
        "CopyInterpreter"
    }

    async fn execute(
        &self,
        mut _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        todo!()
    }
}
