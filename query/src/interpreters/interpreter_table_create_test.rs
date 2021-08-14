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

use common_datavalues::DataType;
use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

use crate::interpreters::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_table_interpreter() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::CreateTable(plan) = PlanParser::create(ctx.clone())
        .build_from_sql("create table default.a(a bigint, b int, c varchar(255), d smallint, e Date ) Engine = Null")?
    {
        let executor = CreateTableInterpreter::try_create(ctx, plan.clone())?;
        assert_eq!(executor.name(), "CreateTableInterpreter");

        assert_eq!(plan.schema().field_with_name("a")?.data_type(), &DataType::Int64);
        assert_eq!(plan.schema().field_with_name("b")?.data_type(), &DataType::Int32);
        assert_eq!(plan.schema().field_with_name("c")?.data_type(), &DataType::Utf8);
        assert_eq!(plan.schema().field_with_name("d")?.data_type(), &DataType::Int16);
        assert_eq!(plan.schema().field_with_name("e")?.data_type(), &DataType::Date32);

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}
