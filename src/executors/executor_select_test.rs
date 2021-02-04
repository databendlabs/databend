// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_select_executor() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;

    use crate::datavalues::*;
    use crate::executors::*;
    use crate::planners::*;
    use crate::sql::*;

    let test_source = crate::tests::NumberTestData::create();
    let ctx =
        crate::contexts::FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;

    if let PlanNode::Select(plan) = PlanParser::create(ctx.clone())
        .build_from_sql("select number from system.numbers_mt(10) where (number+2)<2")?
    {
        let executor = SelectExecutor::try_create(ctx.clone(), plan)?;
        assert_eq!(executor.name(), "SelectExecutor");

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    if let PlanNode::Select(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("select 1 + 1, 2 + 2, 3 * 3, 4 * 4")?
    {
        let executor = SelectExecutor::try_create(ctx.clone(), plan)?;
        assert_eq!(executor.name(), "SelectExecutor");

        let mut stream = executor.execute().await?;
        if let Some(block) = stream.next().await {
            let record_batch = block?.to_arrow_batch()?;
            assert_eq!(1, record_batch.num_rows());
            assert_eq!(4, record_batch.num_columns());

            let sc = record_batch.schema().clone();
            let types: Vec<&DataType> = sc.fields().iter().map(|f| f.data_type()).collect();
            assert_eq!(
                vec![
                    &DataType::UInt64,
                    &DataType::UInt64,
                    &DataType::UInt64,
                    &DataType::UInt64
                ],
                types
            );
        }
        while let Some(_block) = stream.next().await {}
    }

    Ok(())
}
