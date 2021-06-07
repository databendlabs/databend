// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::ArrayRef;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_flights::GetTableActionResult;
use common_flights::StoreClient;
use common_planners::ScanPlan;
use log::info;
use pretty_assertions::assert_eq;
use test_env_log::test;

#[test(tokio::test)]
async fn test_flight_create_database() -> anyhow::Result<()> {
    use common_planners::CreateDatabasePlan;
    use common_planners::DatabaseEngineType;

    // 1. Service starts.
    let addr = crate::tests::start_store_server().await?;

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
        assert_eq!(0, res.database_id, "first database id is 0");
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
        assert_eq!(1, res.database_id, "second database id is 1");
    }

    Ok(())
}

#[test(tokio::test)]
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

    info!("init logging");

    // 1. Service starts.
    let addr = crate::tests::start_store_server().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    {
        // prepare db
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: "db1".to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };

        let res = client.create_database(plan.clone()).await;

        info!("create database res: {:?}", res);

        let res = res.unwrap();
        assert_eq!(0, res.database_id, "first database id is 0");
    }
    {
        // create table and fetch it

        // Table schema with metadata(due to serde issue).
        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "number",
            DataType::UInt64,
            false,
        )]));

        // Create table plan.
        let mut plan = CreateTablePlan {
            if_not_exists: false,
            db: "db1".to_string(),
            table: "tb2".to_string(),
            schema: schema.clone(),
            // TODO check get_table
            options: maplit::hashmap! {"opt‐1".into() => "val-1".into()},
            // TODO
            engine: TableEngineType::JsonEachRaw,
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
                schema: schema.clone(),
            };
            assert_eq!(want, got, "get created table");
        }

        {
            // create table again with if_not_exists = true
            plan.if_not_exists = true;
            let res = client.create_table(plan.clone()).await.unwrap();
            assert_eq!(1, res.table_id, "new table id");

            let got = client.get_table("db1".into(), "tb2".into()).await.unwrap();
            let want = GetTableActionResult {
                table_id: 1,
                db: "db1".into(),
                name: "tb2".into(),
                schema: schema.clone(),
            };
            assert_eq!(want, got, "get created table");
        }

        {
            // create table again with if_not_exists=false
            plan.if_not_exists = false;

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
                table_id: 1,
                db: "db1".into(),
                name: "tb2".into(),
                schema: schema.clone(),
            };
            assert_eq!(want, got, "get old table");
        }
    }

    Ok(())
}

#[test(tokio::test)]
async fn test_do_append() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_arrow::arrow::datatypes::DataType;
    use common_datavalues::DataField;
    use common_datavalues::DataSchema;
    use common_datavalues::Int64Array;
    use common_datavalues::StringArray;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::CreateTablePlan;
    use common_planners::DatabaseEngineType;
    use common_planners::TableEngineType;

    let addr = crate::tests::start_store_server().await?;

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("col_i", DataType::Int64, false),
        DataField::new("col_s", DataType::Utf8, false),
    ]));
    let db_name = "test_db";
    let tbl_name = "test_tbl";

    let col0: ArrayRef = Arc::new(Int64Array::from(vec![0, 1, 2]));
    let col1: ArrayRef = Arc::new(StringArray::from(vec!["str1", "str2", "str3"]));

    let expected_rows = col0.data().len() * 2;
    let expected_cols = 2;

    let block = DataBlock::create_by_array(schema.clone(), vec![col0, col1]);
    let batches = vec![block.clone(), block];
    let num_batch = batches.len();
    let stream = futures::stream::iter(batches);

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;
    {
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };
        client.create_database(plan.clone()).await?;
        let plan = CreateTablePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            table: tbl_name.to_string(),
            schema: schema.clone(),
            options: maplit::hashmap! {"opt‐1".into() => "val-1".into()},
            engine: TableEngineType::Parquet,
        };
        client.create_table(plan.clone()).await?;
    }
    let res = client
        .append_data(
            db_name.to_string(),
            tbl_name.to_string(),
            schema,
            Box::pin(stream),
        )
        .await?;
    log::info!("append res is {:?}", res);
    let summary = res.summary;
    assert_eq!(summary.rows, expected_rows);
    assert_eq!(res.parts.len(), num_batch);
    res.parts.iter().for_each(|p| {
        assert_eq!(p.rows, expected_rows / num_batch);
        assert_eq!(p.cols, expected_cols);
    });
    Ok(())
}
#[test(tokio::test)]
async fn test_scan_partition() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_arrow::arrow::datatypes::DataType;
    use common_datavalues::DataField;
    use common_datavalues::DataSchema;
    use common_datavalues::Int64Array;
    use common_datavalues::StringArray;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::CreateTablePlan;
    use common_planners::DatabaseEngineType;
    use common_planners::TableEngineType;

    let addr = crate::tests::start_store_server().await?;

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("col_i", DataType::Int64, false),
        DataField::new("col_s", DataType::Utf8, false),
    ]));
    let db_name = "test_db";
    let tbl_name = "test_tbl";

    let col0: ArrayRef = Arc::new(Int64Array::from(vec![0, 1, 2]));
    let col1: ArrayRef = Arc::new(StringArray::from(vec!["str1", "str2", "str3"]));

    let expected_rows = col0.data().len() * 2;
    let expected_cols = 2;

    let block = DataBlock::create(schema.clone(), vec![
        DataColumnarValue::Array(col0),
        DataColumnarValue::Array(col1),
    ]);
    let batches = vec![block.clone(), block];
    let num_batch = batches.len();
    let stream = futures::stream::iter(batches);

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;
    {
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };
        client.create_database(plan.clone()).await?;
        let plan = CreateTablePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            table: tbl_name.to_string(),
            schema: schema.clone(),
            options: maplit::hashmap! {"opt‐1".into() => "val-1".into()},
            engine: TableEngineType::Parquet,
        };
        client.create_table(plan.clone()).await?;
    }
    let res = client
        .append_data(
            db_name.to_string(),
            tbl_name.to_string(),
            schema,
            Box::pin(stream),
        )
        .await?;
    log::info!("append res is {:?}", res);
    let summary = res.summary;
    assert_eq!(summary.rows, expected_rows);
    assert_eq!(res.parts.len(), num_batch);
    res.parts.iter().for_each(|p| {
        assert_eq!(p.rows, expected_rows / num_batch);
        assert_eq!(p.cols, expected_cols);
    });

    let plan = ScanPlan {
        schema_name: tbl_name.to_string(),
        ..ScanPlan::empty()
    };
    let res = client
        .scan_partition(db_name.to_string(), tbl_name.to_string(), &plan)
        .await;
    // TODO d assertions, de-duplicated codes
    println!("scan res is {:?}", res);

    Ok(())
}
