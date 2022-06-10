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
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;
use common_planners::UndropTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct UndropTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: UndropTablePlan,
}

impl UndropTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: UndropTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(UndropTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for UndropTableInterpreter {
    fn name(&self) -> &str {
        "UndropTableInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();

        // shall we add UserPrivilege::Type::Undrop ?
        self.ctx
            .get_current_session()
            .validate_privilege(
                &GrantObject::Database(catalog_name.into(), db_name.into()),
                UserPrivilegeType::Drop,
            )
            .await?;

        let catalog = self.ctx.get_catalog(catalog_name)?;
        catalog.undrop_table(self.plan.clone().into()).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
