// Copyright 2020 Datafuse Labs.
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
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_planners::ShowCreateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use log::debug;

use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::DatabendQueryContextRef;

pub struct ShowCreateTableInterpreter {
    ctx: DatabendQueryContextRef,
    plan: ShowCreateTablePlan,
}

impl ShowCreateTableInterpreter {
    pub fn try_create(
        ctx: DatabendQueryContextRef,
        plan: ShowCreateTablePlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowCreateTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowCreateTableInterpreter {
    fn name(&self) -> &str {
        "ShowCreateTableInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let catalog = self.ctx.get_catalog();

        let table = catalog.get_table(&self.plan.db, &self.plan.table).await?;

        let name = table.name();
        let engine = table.engine();
        let schema = table.schema();

        let mut table_info = format!("CREATE TABLE `{}` (\n", name);
        for field in schema.fields().iter() {
            let column = format!("  `{}` {},\n", field.name(), field.data_type());
            table_info.push_str(column.as_str());
        }
        let table_engine = format!(") ENGINE={}", engine);
        table_info.push_str(table_engine.as_str());

        let show_fields = vec![
            DataField::new("Table", DataType::String, false),
            DataField::new("Create Table", DataType::String, false),
        ];
        let show_schema = DataSchemaRefExt::create(show_fields);

        let block = DataBlock::create_by_array(show_schema.clone(), vec![
            Series::new(vec![name.as_bytes()]),
            Series::new(vec![table_info.into_bytes()]),
        ]);
        debug!("Show create table executor result: {:?}", block);

        Ok(Box::pin(DataBlockStream::create(show_schema, None, vec![
            block,
        ])))
    }
}
