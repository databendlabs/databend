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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;
use common_planners::CreateTablePlan;
use common_planners::InsertInputSource;
use common_planners::InsertPlan;
use common_planners::PlanNode;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use super::InsertInterpreter;
use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct CreateTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateTablePlan,
}

impl CreateTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreateTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTableInterpreter {
    fn name(&self) -> &str {
        "CreateTableInterpreter"
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        self.ctx
            .get_current_session()
            .validate_privilege(
                &GrantObject::Database(self.plan.db.clone()),
                UserPrivilegeType::Create,
            )
            .await?;

        match &self.plan.as_select {
            Some(select_plan_node) => {
                self.create_table_as_select(input_stream, select_plan_node.clone())
                    .await
            }
            None => self.create_table().await,
        }
    }
}

impl CreateTableInterpreter {
    async fn create_table_as_select(
        &self,
        input_stream: Option<SendableDataBlockStream>,
        select_plan_node: Box<PlanNode>,
    ) -> Result<SendableDataBlockStream> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog();

        // TODO: maybe the table creation and insertion should be a transaction, but it may require create_table support 2pc.
        catalog.create_table(self.plan.clone().into()).await?;
        let table = catalog
            .get_table(tenant.as_str(), &self.plan.db, &self.plan.table)
            .await?;

        // If the table creation query contains column definitions, like 'CREATE TABLE t1(a int) AS SELECT * from t2',
        // we use the definitions to create the table schema. It may happen that the "AS SELECT" query's schema doesn't
        // match the table's schema. For example,
        //
        //   mysql> create table t2(a int, b int);
        //   mysql> create table t1(x string, y string) as select * from t2;
        //
        // For the situation above, we implicitly cast the data type when inserting data.
        // The casting and schema checking is in interpreter_insert.rs, function check_schema_cast.
        let table_schema = table.schema();
        let select_fields: Vec<DataField> = select_plan_node
            .schema()
            .fields()
            .iter()
            .filter_map(|f| table_schema.field_with_name(f.name()).ok())
            .cloned()
            .collect();
        let schema = DataSchemaRefExt::create(select_fields);
        let insert_plan = InsertPlan {
            database_name: self.plan.db.clone(),
            table_name: self.plan.table.clone(),
            table_id: table.get_id(),
            schema,
            overwrite: false,
            source: InsertInputSource::SelectPlan(select_plan_node),
        };
        let insert_interpreter = InsertInterpreter::try_create(self.ctx.clone(), insert_plan)?;
        insert_interpreter.execute(input_stream).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }

    async fn create_table(&self) -> Result<SendableDataBlockStream> {
        let catalog = self.ctx.get_catalog();
        catalog.create_table(self.plan.clone().into()).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
