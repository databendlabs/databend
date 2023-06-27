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

use common_base::base::tokio;
use common_exception::Result;
use common_expression::type_check::common_super_type;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::SendableDataBlockStream;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::planner::plans::Plan;
use common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::interpreters::InterpreterPtr;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;

async fn plan_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<Plan> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    Ok(plan)
}

async fn get_interpreter(ctx: Arc<QueryContext>, sql: &str) -> Result<InterpreterPtr> {
    let plan = plan_sql(ctx.clone(), sql).await?;
    InterpreterFactory::get(ctx, &plan).await
}

async fn execute_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<SendableDataBlockStream> {
    let interpreter = get_interpreter(ctx.clone(), sql).await?;
    interpreter.execute(ctx).await
}

async fn execute_plan(ctx: Arc<QueryContext>, plan: &Plan) -> Result<SendableDataBlockStream> {
    let interpreter = InterpreterFactory::get(ctx.clone(), plan).await?;
    interpreter.execute(ctx).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_union_output_type() -> Result<()> {
    {
        let fixture = TestFixture::new().await;
        let interpreter =
            get_interpreter(fixture.ctx(), "select 1 union all select 2.0::FLOAT64").await?;
        let schema = interpreter.schema();
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Number(NumberDataType::Float64),
        ));

        let interpreter =
            get_interpreter(fixture.ctx(), "select 1.0::FLOAT64 union all select 2").await?;
        let schema = interpreter.schema();
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Number(NumberDataType::Float64),
        ));
    }

    {
        let fixture = TestFixture::new().await;
        execute_sql(fixture.ctx(), "create table a (a int)").await?;
        execute_sql(fixture.ctx(), "create table b (b double)").await?;
        let interpreter =
            get_interpreter(fixture.ctx(), "select * from a union all select * from b").await?;
        let schema = interpreter.schema();
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Number(NumberDataType::Float64),
        ));

        let interpreter =
            get_interpreter(fixture.ctx(), "select * from b union all select * from a").await?;
        let schema = interpreter.schema();
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Number(NumberDataType::Float64),
        ));
    }

    Ok(())
}

const ALL_CREATABLE_TYPES: &[&str] = &[
    "BOOLEAN",
    "INT8",
    "INT16",
    "INT32",
    "INT64",
    "UINT8",
    "UINT16",
    "UINT32",
    "UINT64",
    "FLOAT32",
    "FLOAT64",
    "STRING",
    "DATE",
    "TIMESTAMP",
];

fn get_created_data_type(plan: &Plan) -> DataType {
    if let Plan::CreateTable(plan) = plan {
        plan.schema.field(0).data_type().into()
    } else {
        unreachable!()
    }
}

fn get_create_table_sql(table_name: &str, ty: &str, nullable: bool) -> String {
    let nullable = if nullable { " null" } else { "" };
    format!("create table {table_name} (a {ty}{nullable})")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_union_output_type() -> Result<()> {
    for ty1 in ALL_CREATABLE_TYPES {
        for nullable1 in [true, false] {
            let table1_sql = get_create_table_sql("t1", ty1, nullable1);
            for ty2 in ALL_CREATABLE_TYPES {
                for nullable2 in [true, false] {
                    let table2_sql = get_create_table_sql("t2", ty2, nullable2);

                    {
                        let fixture = TestFixture::new().await;
                        let plan1 = plan_sql(fixture.ctx(), &table1_sql).await?;
                        let ty1 = get_created_data_type(&plan1);
                        let plan2 = plan_sql(fixture.ctx(), &table2_sql).await?;
                        let ty2 = get_created_data_type(&plan2);

                        if let Some(common_type) =
                            common_super_type(ty1, ty2, &BUILTIN_FUNCTIONS.default_cast_rules)
                        {
                            execute_plan(fixture.ctx(), &plan1).await?;
                            execute_plan(fixture.ctx(), &plan2).await?;
                            let select = get_interpreter(
                                fixture.ctx(),
                                "select * from t1 union all select * from t2",
                            )
                            .await?;
                            let schema = select.schema();
                            assert_eq!(schema.field(0).data_type(), &common_type);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
