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
use common_exception::Result;
use common_planners::DescribeTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::DatafuseQueryContextRef;

pub struct DescribeTableInterpreter {
    ctx: DatafuseQueryContextRef,
    plan: DescribeTablePlan,
}

impl DescribeTableInterpreter {
    pub fn try_create(
        ctx: DatafuseQueryContextRef,
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
        let table = self
            .ctx
            .get_table(self.plan.db.as_str(), self.plan.table.as_str())?;
        let schema = table.raw().schema()?;

        let mut names: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        let mut nulls: Vec<String> = vec![];
        for field in schema.fields().iter() {
            names.push(field.name().to_string());
            types.push(format!("{:?}", field.data_type()));
            nulls.push(if field.is_nullable() {
                "YES".to_string()
            } else {
                "NO".to_string()
            });
        }
        let names: Vec<&str> = names.iter().map(|x| x.as_str()).collect();
        let types: Vec<&str> = types.iter().map(|x| x.as_str()).collect();
        let nulls: Vec<&str> = nulls.iter().map(|x| x.as_str()).collect();

        let desc_schema = self.plan.schema();

        let block = DataBlock::create_by_array(desc_schema.clone(), vec![
            Series::new(names),
            Series::new(types),
            Series::new(nulls),
        ]);

        Ok(Box::pin(DataBlockStream::create(desc_schema, None, vec![
            block,
        ])))
    }
}
