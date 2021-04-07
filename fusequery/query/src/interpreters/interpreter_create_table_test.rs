// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_table_interpreter() -> anyhow::Result<()> {
    use common_datavalues::DataType;
    use common_planners::*;
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;

    use crate::interpreters::*;
    use crate::sql::*;

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
