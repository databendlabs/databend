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
use common_exception::Result;
use common_planners::ShowCreateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use tracing::debug;

use crate::interpreters::Interpreter;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalScalar;
use crate::sql::is_internal_opt_key;

pub struct ShowCreateTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowCreateTablePlan,
}

impl ShowCreateTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowCreateTablePlan) -> Result<Self> {
        Ok(ShowCreateTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowCreateTableInterpreter {
    fn name(&self) -> &str {
        "ShowCreateTableInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str())?;

        let table = catalog
            .get_table(tenant.as_str(), &self.plan.database, &self.plan.table)
            .await?;

        let name = table.name();
        let engine = table.engine();
        let schema = table.schema();
        let field_comments = table.field_comments();
        let n_fields = schema.fields().len();

        let mut table_create_sql = format!("CREATE TABLE `{}` (\n", name);

        // Append columns.
        {
            let mut columns = vec![];
            for (idx, field) in schema.fields().iter().enumerate() {
                let default_expr = match field.default_expr() {
                    Some(expr) => {
                        let expression: PhysicalScalar = serde_json::from_str(expr)?;
                        format!(" DEFAULT {expression}")
                    }
                    None => "".to_string(),
                };
                // compatibility: creating table in the old planner will not have `fields_comments`
                let comment = if field_comments.len() == n_fields && !field_comments[idx].is_empty()
                {
                    // make the display more readable.
                    format!(
                        " COMMENT '{}'",
                        &field_comments[idx].as_str().replace('\'', "\\'")
                    )
                } else {
                    "".to_string()
                };
                let column = format!(
                    "  `{}` {}{}{}",
                    field.name(),
                    format_data_type_sql(field.data_type()),
                    default_expr,
                    comment
                );

                columns.push(column);
            }
            // Format is:
            //  (
            //      x,
            //      y
            //  )
            let columns_str = format!("{}\n", columns.join(",\n"));
            table_create_sql.push_str(&columns_str);
        }

        let table_engine = format!(") ENGINE={}", engine);
        table_create_sql.push_str(table_engine.as_str());

        let table_info = table.get_table_info();
        if let Some((_, cluster_keys_str)) = table_info.meta.cluster_key() {
            table_create_sql.push_str(format!(" CLUSTER BY {}", cluster_keys_str).as_str());
        }

        table_create_sql.push_str({
            let mut opts = table_info.options().iter().collect::<Vec<_>>();
            opts.sort_by_key(|(k, _)| *k);
            opts.iter()
                .filter(|(k, _)| !is_internal_opt_key(k))
                .map(|(k, v)| format!(" {}='{}'", k.to_uppercase(), v))
                .collect::<Vec<_>>()
                .join("")
                .as_str()
        });

        let show_schema = self.plan.schema();
        let block = DataBlock::create(show_schema.clone(), vec![
            Series::from_data(vec![name.as_bytes()]),
            Series::from_data(vec![table_create_sql.into_bytes()]),
        ]);
        debug!("Show create table executor result: {:?}", block);

        Ok(Box::pin(DataBlockStream::create(show_schema, None, vec![
            block,
        ])))
    }
}
