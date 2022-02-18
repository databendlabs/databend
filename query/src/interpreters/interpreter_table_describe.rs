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
use common_planners::DescribeTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct DescribeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescribeTablePlan,
}

impl DescribeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescribeTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(DescribeTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for DescribeTableInterpreter {
    fn name(&self) -> &str {
        "DescribeTableInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let database = self.plan.db.as_str();
        let table = self.plan.table.as_str();
        let table = self.ctx.get_table(database, table).await?;
        let schema = table.schema();

        let mut names: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        let mut nulls: Vec<String> = vec![];
        for field in schema.fields().iter() {
            names.push(field.name().to_string());
            types.push(format!("{:?}", remove_nullable(field.data_type())));
            nulls.push(if field.is_nullable() {
                "YES".to_string()
            } else {
                "NO".to_string()
            });
        }
        let names: Vec<&[u8]> = names.iter().map(|x| x.as_bytes()).collect();
        let types: Vec<&[u8]> = types.iter().map(|x| x.as_bytes()).collect();
        let nulls: Vec<&[u8]> = nulls.iter().map(|x| x.as_bytes()).collect();

        let desc_schema = self.plan.schema();

        let block = DataBlock::create(desc_schema.clone(), vec![
            Series::from_data(names),
            Series::from_data(types),
            Series::from_data(nulls),
        ]);

        Ok(Box::pin(DataBlockStream::create(desc_schema, None, vec![
            block,
        ])))
    }
}
