// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_flights::GetTableActionResult;
use log::info;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_database() -> anyhow::Result<()> {
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
            options: Default::default()
        };

        let res = client.create_database(plan.clone()).await;
        info!("create database res: {:?}", res);
        let res = res.unwrap();
        assert_eq!(0, res.database_id, "first database id is 0");
    }
    {
        // create second db
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: "db2".to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default()
        };

        let res = client.create_database(plan.clone()).await;
        info!("create database res: {:?}", res);
        let res = res.unwrap();
        assert_eq!(1, res.database_id, "second database id is 1");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_get_table() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_arrow::arrow::datatypes::DataType;
    use common_datavalues::DataField;
    use common_datavalues::DataSchema;
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
        // prepare db
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: "db1".to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default()
        };

        let res = client.create_database(plan.clone()).await;

        info!("create database res: {:?}", res);

        let res = res.unwrap();
        assert_eq!(0, res.database_id, "first database id is 0");
    }
    {
        // create table and fetch it

        // Table schema with metadata(due to serde issue).
        let schema = Arc::new(DataSchema::new_with_metadata(
            vec![DataField::new("number", DataType::UInt64, false)],
            [("Key".to_string(), "Value".to_string())]
                .iter()
                .cloned()
                .collect()
        ));

        // Create table plan.
        let mut plan = CreateTablePlan {
            if_not_exists: false,
            db: "db1".to_string(),
            table: "tb2".to_string(),
            schema: schema.clone(),
            // TODO check get_table
            options: maplit::hashmap! {"opt‐1".into() => "val-1".into()},
            // TODO
            engine: TableEngineType::JsonEachRaw
        };

        {
            // create table OK
            let res = client.create_table(plan.clone()).await.unwrap();
            assert_eq!(1, res.table_id, "table id is 1");

            let got = client.get_table("db1".into(), "tb2".into()).await.unwrap();
            let want = GetTableActionResult {
                table_id: 1,
                db: "db1".into(),
                name: "tb2".into(),
                schema: schema.clone()
            };
            assert_eq!(want, got, "get created table");
        }

        {
            // create table again, override, with if_not_exists = false
            let res = client.create_table(plan.clone()).await.unwrap();
            assert_eq!(2, res.table_id, "new table id");

            let got = client.get_table("db1".into(), "tb2".into()).await.unwrap();
            let want = GetTableActionResult {
                table_id: 2,
                db: "db1".into(),
                name: "tb2".into(),
                schema: schema.clone()
            };
            assert_eq!(want, got, "get created table");
        }

        {
            // create table with if_not_exists=true
            plan.if_not_exists = true;

            let res = client.create_table(plan.clone()).await;
            info!("create table res: {:?}", res);

            let status = res.err().unwrap();
            assert_eq!(
                "status: Some entity that we attempted to create already exists: table exists",
                status.to_string()
            );

            // get_table returns the old table

            let got = client.get_table("db1".into(), "tb2".into()).await.unwrap();
            let want = GetTableActionResult {
                table_id: 2,
                db: "db1".into(),
                name: "tb2".into(),
                schema: schema.clone()
            };
            assert_eq!(want, got, "get old table");
        }
    }

    Ok(())
}
