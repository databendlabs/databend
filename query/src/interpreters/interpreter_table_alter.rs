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
use common_planners::AlterTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct AlterTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterTablePlan,
}

impl AlterTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(AlterTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterTableInterpreter {
    fn name(&self) -> &str {
        "AlterTableInterpreter"
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        self.ctx.get_current_session().validate_privilege(
            &GrantObject::Table(
                self.plan.database_name.clone(),
                self.plan.table_name.clone(),
            ),
            UserPrivilegeType::Alter,
        )?;
        todo!()
    }
}

impl AlterTableInterpreter {
    async fn apply(&self) -> Result<()> {
        match &self.plan.operation {
            common_planners::AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                self.rename_column(&old_column_name, &new_column_name)
                    .await?;
                Ok(())
            }
            _ => todo!(),
        }
    }
    async fn rename_column(
        &self,
        old_col_name: &str,
        new_col_name: &str,
    ) -> Result<SendableDataBlockStream> {
        let catalog = self.ctx.get_catalog();
        let tenant = self.ctx.get_tenant();
        let db_name = self.plan.database_name.as_str();
        let tbl_name = self.plan.table_name.as_str();
        let tbl = catalog.get_table(&tenant, db_name, tbl_name).await?;
        let schema = tbl.get_table_info().schema();
        //todo!()
        //let new_schema = schema.rename(old_col_name, new_col_name)?;
        //let tbl.update_schema()

        //        catalog.create_table(self.plan.clone().into()).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
