// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_database() -> anyhow::Result<()> {
    use common_planners::{CreateDatabasePlan, DatabaseEngineType};

    // 1. Service starts.
    let addr = crate::tests::start_one_service().await?;

    // 2. Create database.
    let plan = CreateDatabasePlan {
        if_not_exists: false,
        db: "db1".to_string(),
        engine: DatabaseEngineType::Local,
        options: Default::default(),
    };
    let mut client = crate::tests::FlightClient::try_create(addr.to_string()).await?;
    let res = client.create_database(plan.clone()).await;
    assert!(res.is_err());

    Ok(())
}
