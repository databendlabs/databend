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
use common_planners::DescribeTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::FuseQueryContextRef;

pub struct DescribeTableInterpreter {
    ctx: FuseQueryContextRef,
    plan: DescribeTablePlan,
}

impl DescribeTableInterpreter {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        plan: DescribeTablePlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(DescribeTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for DescribeTableInterpreter {
    fn name(&self) -> &str {
        "DescribeTableInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let datasource = self.ctx.get_datasource();
        let database = datasource.get_database(self.plan.db.as_str())?;
        let table = database.get_table(self.plan.table.as_str())?;

        let desc_schema = DataSchemaRefExt::create(
            vec![
                DataField::new("Field", DataType::Utf8, false),
                DataField::new("Type", DataType::Utf8, false),
                DataField::new("Null", DataType::Utf8, false),
            ],
        );

        let mut names: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        let mut nulls: Vec<String> = vec![];
        let schema = table.schema()?;
        for field in schema.fields().iter() {
            names.push(field.name().to_string());
            types.push(format!("{:?}", field.data_type()));
            nulls.push(if field.is_nullable() { "YES".to_string() } else { "NO".to_string() });
        }

        let names: Vec<&str> = names.iter().map(|x| x.as_str()).collect();
        let types: Vec<&str> = types.iter().map(|x| x.as_str()).collect();
        let nulls: Vec<&str> = nulls.iter().map(|x| x.as_str()).collect();

        let block = DataBlock::create_by_array(desc_schema.clone(), vec![
            Series::new(names),
            Series::new(types),
            Series::new(nulls),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            desc_schema,
            None,
            vec![block],
        )))
    }
}
