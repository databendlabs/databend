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

use common_datavalues::DataType;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;
use common_planners::AlterTablePlan;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;
use crate::storages::Table;

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
        _input_stream: Option<SendableDataBlockStream>,
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
        use common_planners::AlterTableOperation::*;
        match &self.plan.operation {
            RenameColumn {
                old_column_name,
                new_column_name,
            } => self.rename_column(&old_column_name, &new_column_name).await,
            AddColumn {
                column_name,
                data_type,
            } => self.add_column(column_name.as_str(), data_type).await,

            _ => todo!(),
        }
    }

    async fn add_column(&self, col_name: &str, data_type: &DataType) -> Result<()> {
        todo!()
    }

    async fn rename_column(&self, old_col_name: &str, new_col_name: &str) -> Result<()> {
        let table = self.get_table().await?;
        table
            .rename(self.ctx.clone(), old_col_name, new_col_name)
            .await
    }

    async fn get_table(&self) -> Result<Arc<dyn Table>> {
        let catalog = self.ctx.get_catalog();
        let tenant = self.ctx.get_tenant();
        let db_name = self.plan.database_name.as_str();
        let tbl_name = self.plan.table_name.as_str();
        catalog.get_table(&tenant, db_name, tbl_name).await
    }
}
