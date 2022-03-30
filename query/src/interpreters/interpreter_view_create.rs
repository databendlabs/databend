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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;
use common_planners::CreateTablePlan;
use common_planners::InsertInputSource;
use common_planners::InsertPlan;
use common_planners::PlanNode;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;


use common_planners::CreateViewPlan;
use super::InsertInterpreter;
use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct CreateViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateViewPlan,
}

impl CreateViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateViewPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreateViewInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateViewInterpreter {
    fn name(&self) -> &str {
        "CreateViewInterpreter"
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        todo!()
    }
}
