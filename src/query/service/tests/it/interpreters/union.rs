// Copyright 2021 Datafuse Labs
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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SendableDataBlockStream;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::planner::plans::Plan;
use databend_common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::interpreters::InterpreterPtr;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;

async fn plan_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<Plan> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    Ok(plan)
}

async fn get_interpreter(
    ctx: Arc<QueryContext>,
    sql: &str,
) -> Result<(InterpreterPtr, DataSchemaRef)> {
    let plan = plan_sql(ctx.clone(), sql).await?;
    let it = InterpreterFactory::get(ctx, &plan).await?;
    Ok((it, plan.schema()))
}

async fn execute_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<SendableDataBlockStream> {
    let (interpreter, _) = get_interpreter(ctx.clone(), sql).await?;
    interpreter.execute(ctx).await
}

async fn execute_plan(ctx: Arc<QueryContext>, plan: &Plan) -> Result<SendableDataBlockStream> {
    let interpreter = InterpreterFactory::get(ctx.clone(), plan).await?;
    interpreter.execute(ctx).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_union_output_type() -> Result<()> {
    {
        let fixture = TestFixture::setup().await?;

        let (_, schema) = get_interpreter(
            fixture.new_query_ctx().await?,
            "select 1 union all select 2.0::FLOAT64",
        )
        .await?;
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Number(NumberDataType::Float64),
        ));

        let (_, schema) = get_interpreter(
            fixture.new_query_ctx().await?,
            "select 1.0::FLOAT64 union all select 2",
        )
        .await?;

        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Number(NumberDataType::Float64),
        ));
    }

    {
        let fixture = TestFixture::setup().await?;

        execute_sql(fixture.new_query_ctx().await?, "create table a (a int)").await?;
        execute_sql(fixture.new_query_ctx().await?, "create table b (b double)").await?;
        let (_, schema) = get_interpreter(
            fixture.new_query_ctx().await?,
            "select * from a union all select * from b",
        )
        .await?;
        assert!(matches!(
            schema.field(0).data_type().remove_nullable(),
            DataType::Number(NumberDataType::Float64),
        ));

        let (_, schema) = get_interpreter(
            fixture.new_query_ctx().await?,
            "select * from b union all select * from a",
        )
        .await?;
        assert!(matches!(
            schema.field(0).data_type().remove_nullable(),
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

fn table_schema(plan: &Plan) -> TableSchemaRef {
    if let Plan::CreateTable(plan) = plan {
        plan.schema.clone()
    } else {
        unreachable!()
    }
}

fn field_name(ty: &str, nullable: bool) -> String {
    format!(
        "f_{}_{}",
        ty.to_lowercase(),
        if nullable { "null" } else { "not_null" }
    )
}

fn create_all_types_table_sql(table_name: &str) -> String {
    let mut sql = format!("create table {} (", table_name);
    for (i, ty) in ALL_CREATABLE_TYPES.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("{} {}", field_name(ty, false), ty));
    }
    for ty in ALL_CREATABLE_TYPES.iter() {
        sql.push_str(", ");
        sql.push_str(&format!("{} {} NULL", field_name(ty, true), ty));
    }
    sql.push(')');
    sql
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_union_output_type() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    // Prepare tables
    let sql1 = create_all_types_table_sql("t1");
    let plan1 = plan_sql(fixture.new_query_ctx().await?, &sql1).await?;
    execute_plan(fixture.new_query_ctx().await?, &plan1).await?;
    let sql2 = create_all_types_table_sql("t2");
    let plan2 = plan_sql(fixture.new_query_ctx().await?, &sql2).await?;
    execute_plan(fixture.new_query_ctx().await?, &plan2).await?;

    let table_schema = table_schema(&plan1);
    let table_fields = table_schema.fields();

    for f1 in table_fields.iter() {
        let name1 = f1.name();
        let ty1: DataType = f1.data_type().into();

        for f2 in table_fields.iter() {
            let name2 = f2.name();
            let ty2: DataType = f2.data_type().into();

            if let Some(common_type) = common_super_type(
                ty1.clone(),
                ty2.clone(),
                &BUILTIN_FUNCTIONS.default_cast_rules,
            ) {
                let (_, schema) = get_interpreter(
                    fixture.new_query_ctx().await?,
                    &format!("select {name1} from t1 union all select {name2} from t2",),
                )
                .await?;

                assert_eq!(
                    schema.field(0).data_type(),
                    &common_type,
                    "field1: {name1}, field2: {name2}"
                );
            }
        }
    }

    Ok(())
}
