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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::series::Series;
use common_exception::Result;
use common_planners::ShowGrantsPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct ShowGrantsInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowGrantsPlan,
}

impl ShowGrantsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowGrantsPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowGrantsInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowGrantsInterpreter {
    fn name(&self) -> &str {
        "ShowGrantsInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let schema =
            DataSchemaRefExt::create(vec![DataField::new("Grants", DataType::String, false)]);

        // TODO: add permission check on reading user grants
        let user_info = match self.plan.user_identity {
            None => self.ctx.get_current_user()?,
            Some(ref user_identity) => {
                self.ctx
                    .get_sessions_manager()
                    .get_user_manager()
                    .get_user(&user_identity.username, &user_identity.hostname)
                    .await?
            }
        };

        let grant_list = user_info
            .grants
            .entries()
            .iter()
            .map(|e| e.to_string().into_bytes())
            .collect::<Vec<_>>();

        let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(grant_list)]);
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
