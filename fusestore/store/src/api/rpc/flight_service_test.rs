// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use log::info;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_database() -> anyhow::Result<()> {
    use common_flights::store_do_action::StoreDoActionResult;
    use common_flights::CreateDatabaseActionResult;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::DatabaseEngineType;

    // 1. Service starts.
    let addr = crate::tests::start_one_service().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    // 2. Create database.

    {
        // create first db
        let plan = CreateDatabasePlan {
            // TODO test if_not_exists
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
    use common_flights::store_do_action::StoreDoActionResult;
    use common_flights::CreateDatabaseActionResult;
    use common_flights::CreateTableActionResult;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::CreateTablePlan;
    use common_planners::DatabaseEngineType;
    use common_planners::TableEngineType;

    env_logger::init();
    info!("init logging");

    // 1. Service starts.
    let addr = crate::tests::start_one_service().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    {
        // create db
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

        // Table schema with metadata(due to serde issue).
        let schema = Arc::new(DataSchema::new_with_metadata(
            vec![DataField::new("number", DataType::UInt64, false)],
            [("Key".to_string(), "Value".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));

        // Create table plan.
        let mut plan = CreateTablePlan {
            // TODO
            if_not_exists: false,
            db: "db1".to_string(),
            table: "tb2".to_string(),
            schema,
            // TODO
            engine: TableEngineType::JsonEachRaw,
            // TODO
            options: Default::default(),
        };

        {
            // create table OK
            let res = client.create_table(plan.clone()).await;
            info!("create table res: {:?}", res);

            let res = res.unwrap();
            match res {
                StoreDoActionResult::CreateTable(rst) => {
                    let CreateTableActionResult { table_id } = rst;
                    assert_eq!(1, table_id, "table id is 1");
                }
                _ => panic!("expect CreateTableActionResult"),
            }
        }

        {
            // create table again, override
            // TODO fetch table to confirm it is updated
            let res = client.create_table(plan.clone()).await;
            info!("create table res: {:?}", res);

            let res = res.unwrap();
            match res {
                StoreDoActionResult::CreateTable(rst) => {
                    let CreateTableActionResult { table_id } = rst;
                    assert_eq!(2, table_id, "new table id");
                }
                _ => panic!("expect CreateTableActionResult"),
            }
        }

        {
            // create table with if_not_exists=true
            plan.if_not_exists = true;

            // TODO fetch table to confirm it is NOT updated
            let res = client.create_table(plan.clone()).await;
            info!("create table res: {:?}", res);

            let status = res.err().unwrap();
            assert_eq!(
                "status: Some entity that we attempted to create already exists: table exists",
                status.to_string()
            );
        }
    }

    Ok(())
}
