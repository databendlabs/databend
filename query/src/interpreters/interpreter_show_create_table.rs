// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

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

use crate::catalogs::catalog::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::FuseQueryContextRef;

pub struct ShowCreateTableInterpreter {
    ctx: FuseQueryContextRef,
    plan: ShowCreateTablePlan,
}

impl ShowCreateTableInterpreter {
    pub fn try_create(
        ctx: FuseQueryContextRef,
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
        let datasource = self.ctx.get_datasource();
        let database = datasource.get_database(self.plan.db.as_str())?;
        let table_meta = database.get_table(self.plan.table.as_str())?;
        let table = table_meta.datasource();

        let name = table.name();
        let engine = table.engine();
        let schema = table.schema()?;

        let mut table_info = format!("CREATE TABLE `{}` (\n", name);
        for field in schema.fields().iter() {
            let column = format!("  `{}` {},\n", field.name(), field.data_type());
            table_info.push_str(column.as_str());
        }
        let table_engine = format!(") ENGINE={}", engine);
        table_info.push_str(table_engine.as_str());

        let show_fields = vec![
            DataField::new("Table", DataType::Utf8, false),
            DataField::new("Create Table", DataType::Utf8, false),
        ];
        let show_schema = DataSchemaRefExt::create(show_fields);

        let block = DataBlock::create_by_array(show_schema.clone(), vec![
            Series::new(vec![name]),
            Series::new(vec![table_info]),
        ]);
        debug!("Show create table executor result: {:?}", block);

        Ok(Box::pin(DataBlockStream::create(show_schema, None, vec![
            block,
        ])))
    }
}
