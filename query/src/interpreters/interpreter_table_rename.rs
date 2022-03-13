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
use common_planners::RenameTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct RenameTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: RenameTablePlan,
}

impl RenameTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RenameTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(RenameTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for RenameTableInterpreter {
    fn name(&self) -> &str {
        "RenameTableInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        // TODO un-comment below code to check privileges
        // let db_name = self.plan.db.as_str();
        // let tbl_name = self.plan.table_name.as_str();

        // self.ctx
        //     .get_current_session()
        //     .validate_privileges(
        //         &GrantObject::Table(db_name.into(), tbl_name.into()),
        //         vec![UserPrivilegeType::Alter, UserPrivilegeType::Drop],
        //     )
        //     .await?;

        // let new_db_name = self.plan.new_db.as_str();
        // let new_tbl_name = self.plan.new_table_name.as_str();

        // self.ctx
        //     .get_current_session()
        //     .validate_privileges(
        //         &GrantObject::Table(new_db_name.into(), new_tbl_name.into()),
        //         vec![UserPrivilegeType::Create, UserPrivilegeType::Insert],
        //     )
        //     .await?;

        let catalog = self.ctx.get_catalog();
        catalog.rename_table(self.plan.clone().into()).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
