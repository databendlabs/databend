// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_remote() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;

    use crate::datavalues::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let remote_addr = crate::tests::try_start_service(1).await?[0].clone();

    let plan = PlanBuilder::from(
        ctx.clone(),
        &PlanNode::ReadSource(test_source.number_read_source_plan_for_test(100)?),
    )
    .filter(col("number").eq(lit(99)))?
    .build()?;

    let remote = RemoteTransform::try_create(ctx.get_id()?, remote_addr, plan)?;
    let mut stream = remote.execute().await?;
    while let Some(v) = stream.next().await {
        let v = v?;
        if v.num_rows() > 0 {
            let actual = v.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
            let expect = &UInt64Array::from(vec![99]);
            assert_eq!(expect.clone(), actual.clone());
        }
    }
    Ok(())
}
