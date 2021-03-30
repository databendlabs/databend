// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grpc_service_ping() -> Result<(), Box<dyn std::error::Error>> {
    use pretty_assertions::assert_eq;

    use crate::api::rpc::GrpcClient;

    // Test service starts.
    let addr = crate::tests::try_start_service(1).await?[0].clone();

    let client = GrpcClient::create(addr);
    let actual = client.ping("datafuse".to_string()).await?;
    let expect = "Hello datafuse!".to_string();
    assert_eq!(actual, expect);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grpc_service_fetch_partition() -> Result<(), Box<dyn std::error::Error>> {
    use pretty_assertions::assert_eq;

    use crate::api::rpc::GrpcClient;
    use crate::pipelines::processors::PipelineBuilder;
    use crate::sql::PlanParser;

    // 1. Service starts.
    let (addr, session_mgr) = crate::tests::try_start_service_with_session_mgr().await?;
    let ctx = session_mgr.try_create_context()?;

    // 2. Make some partitions for the current context.
    let plan =
        PlanParser::create(ctx.clone()).build_from_sql("select * from system.numbers_mt(80000)")?;
    let _pipeline = PipelineBuilder::create(ctx.clone(), plan).build()?;

    // 3. Fetch the partitions from the context by ID via the gRPC.
    let client = GrpcClient::create(addr);
    let actual = client.fetch_partition(1, ctx.get_id()?).await?;

    // 4. Check.
    assert_eq!(1, actual.len());

    Ok(())
}
