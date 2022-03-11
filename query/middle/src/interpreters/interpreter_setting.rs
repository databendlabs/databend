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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::SettingPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct SettingInterpreter {
    ctx: Arc<QueryContext>,
    set: SettingPlan,
}

impl SettingInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, set: SettingPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(SettingInterpreter { ctx, set }))
    }
}

#[async_trait::async_trait]
impl Interpreter for SettingInterpreter {
    fn name(&self) -> &str {
        "SettingInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.set.clone();
        for var in plan.vars {
            match var.variable.to_lowercase().as_str() {
                // To be compatible with some drivers
                "sql_mode" | "autocommit" => {}
                _ => {
                    self.ctx
                        .get_settings()
                        .set_settings(var.variable, var.value, false)?;
                }
            }
        }

        let schema = DataSchemaRefExt::create(vec![DataField::new("set", Vu8::to_data_type())]);
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
