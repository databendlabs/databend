// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_flights::store_do_action::StoreDoActionResult;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_database() -> anyhow::Result<()> {
    use common_flights::CreateDatabaseActionResult;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::DatabaseEngineType;
    use log::info;
    use pretty_assertions::assert_eq;

    // 1. Service starts.
    let addr = crate::tests::start_one_service().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    // 2. Create database.

    {
        // create first db
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: "db1".to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };

        let res = client.create_database(plan.clone()).await;
        info!("create database res: {:?}", res);
        let res = res.unwrap();
        match res {
            StoreDoActionResult::CreateDatabase(rst) => {
                let CreateDatabaseActionResult { database_id } = rst;
                assert_eq!(0, database_id, "first database id is 0");
            }
            _ => panic!("expect CreateDatabaseActionResult"),
        }
    }
    {
        // create second db
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: "db2".to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };

        let res = client.create_database(plan.clone()).await;
        info!("create database res: {:?}", res);
        let res = res.unwrap();
        match res {
            StoreDoActionResult::CreateDatabase(rst) => {
                let CreateDatabaseActionResult { database_id } = rst;
                assert_eq!(1, database_id, "second database id is 1");
            }
            _ => panic!("expect CreateDatabaseActionResult"),
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_table() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_arrow::arrow::datatypes::DataType;
    use common_datavalues::DataField;
    use common_datavalues::DataSchema;
    use common_flights::StoreClient;
    use common_planners::CreateTablePlan;
    use common_planners::TableEngineType;

    // 1. Service starts.
    let addr = crate::tests::start_one_service().await?;

    // Table schema with metadata(due to serde issue).
    let schema = Arc::new(DataSchema::new_with_metadata(
        vec![DataField::new("number", DataType::UInt64, false)],
        [("Key".to_string(), "Value".to_string())]
            .iter()
            .cloned()
            .collect(),
    ));

    // 2. Create table.
    let plan = CreateTablePlan {
        if_not_exists: false,
        db: "db1".to_string(),
        table: "tb2".to_string(),
        schema,
        engine: TableEngineType::JsonEachRaw,
        options: Default::default(),
    };

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;
    let res = client.create_table(plan.clone()).await;
    assert!(res.is_err());

    Ok(())
}
