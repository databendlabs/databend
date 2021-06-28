// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::ArrayRef;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_flights::StoreClient;
use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;
use common_planners::ScanPlan;
use common_runtime::tokio;
use common_store_api::GetTableActionResult;
use common_store_api::KVApi;
use common_store_api::MetaApi;
use common_store_api::StorageApi;
use common_tracing::tracing;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_database() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    // 1. Service starts.
    let addr = crate::tests::start_store_server().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    // 2. Create database.

    // TODO: test arg if_not_exists: It should respond  an ErrorCode
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
        tracing::info!("create database res: {:?}", res);
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
        tracing::info!("create database res: {:?}", res);
        let res = res.unwrap();
        assert_eq!(1, res.database_id, "second database id is 1");
    }

    // 3. Get database.

    {
        // get present db
        let res = client.get_database("db1").await;
        tracing::debug!("get present database res: {:?}", res);
        let res = res?;
        assert_eq!(0, res.database_id, "db1 id is 0");
        assert_eq!("db1".to_string(), res.db, "db1.db is db1");
    }

    {
        // get absent db
        let res = client.get_database("ghost").await;
        tracing::debug!("=== get absent database res: {:?}", res);
        assert!(res.is_err());
        let res = res.unwrap_err();
        assert_eq!(3, res.code());
        assert_eq!("ghost".to_string(), res.message());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_get_table() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();
    use std::sync::Arc;

    use common_arrow::arrow::datatypes::DataType;
    use common_datavalues::DataField;
    use common_datavalues::DataSchema;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::CreateTablePlan;
    use common_planners::DatabaseEngineType;
    use common_planners::TableEngineType;

    tracing::info!("init logging");

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

        tracing::info!("create database res: {:?}", res);

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
            tracing::info!("create table res: {:?}", res);

            let status = res.err().unwrap();
            assert_eq!(
                "Code: 4003, displayText = table exists.",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_append() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();
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
    tracing::info!("append res is {:?}", res);
    let summary = res.summary;
    assert_eq!(summary.rows, expected_rows);
    assert_eq!(res.parts.len(), num_batch);
    res.parts.iter().for_each(|p| {
        assert_eq!(p.rows, expected_rows / num_batch);
        assert_eq!(p.cols, expected_cols);
    });
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scan_partition() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();
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
    tracing::info!("append res is {:?}", res);
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
        .read_plan(db_name.to_string(), tbl_name.to_string(), &plan)
        .await;
    // TODO d assertions, de-duplicated codes
    println!("scan res is {:?}", res);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let addr = crate::tests::start_store_server().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    {
        // write
        let res = client
            .upsert_kv("foo", None, "bar".to_string().into_bytes())
            .await?;
        assert_eq!(None, res.prev);
        assert_eq!(Some((1, "bar".to_string().into_bytes())), res.result);
    }

    {
        // write fails with unmatched seq
        let res = client
            .upsert_kv("foo", Some(2), "bar".to_string().into_bytes())
            .await?;
        assert_eq!(
            Some((1, "bar".to_string().into_bytes())),
            res.prev,
            "old value"
        );
        assert_eq!(None, res.result, "Nothing changed");
    }

    {
        // write done with matching seq
        let res = client
            .upsert_kv("foo", Some(1), "wow".to_string().into_bytes())
            .await?;
        assert_eq!(
            Some((1, "bar".to_string().into_bytes())),
            res.prev,
            "old value"
        );
        assert_eq!(
            Some((2, "wow".to_string().into_bytes())),
            res.result,
            "new value"
        );
    }

    // mget

    {
        let res = client.get_kv("foo").await?;
        assert_eq!(Some((2, "wow".to_string().into_bytes())), res.result);

        client
            .upsert_kv("another_key", None, "value of ak".to_string().into_bytes())
            .await?;
        let res = client
            .mget_kv(&vec!["foo".to_string(), "another_key".to_string()])
            .await?;
        assert_eq!(res.result, vec![
            Some((2, "wow".to_string().into_bytes())),
            // NOTE, the sequence number is increased globally (inside the namespace of generic kv)
            Some((3, "value of ak".to_string().into_bytes())),
        ]);

        let res = client
            .mget_kv(&vec!["foo".to_string(), "key_no exist".to_string()])
            .await?;
        assert_eq!(res.result, vec![
            Some((2, "wow".to_string().into_bytes())),
            None
        ]);
    }

    // prefix list

    let mut values = vec![];
    {
        client.upsert_kv("t", None, "".as_bytes().to_vec()).await?;

        for i in 0..9 {
            let key = format!("__users/{}", i);
            let val = format!("val_{}", i);
            values.push(val.clone());
            client
                .upsert_kv(&key, None, val.as_bytes().to_vec())
                .await?;
        }
        client.upsert_kv("v", None, "".as_bytes().to_vec()).await?;
    }

    let res = client.prefix_list_kv("__users/").await?;
    assert_eq!(
        res.iter().map(|i| i.1.clone()).collect::<Vec<_>>(),
        values
            .iter()
            .map(|v| v.as_bytes().to_vec())
            .collect::<Vec<_>>()
    );

    // delete
    {
        let test_key = "test_key";
        client
            .upsert_kv(test_key, None, "value of ak".to_string().into_bytes())
            .await?;

        let current = client.get_kv(test_key).await?;
        if let Some((seq, _val)) = current.result {
            // seq mismatch
            let wrong_seq = Some(seq + 1);
            let res = client.delete_kv(test_key, wrong_seq).await?;
            assert!(res.is_none());

            // seq match
            let res = client.delete_kv(test_key, Some(seq)).await?;
            assert!(res.is_some());

            // read nothing
            let r = client.get_kv(test_key).await?;
            assert!(r.result.is_none());
        } else {
            panic!("expecting a value, but got nothing");
        }

        // key not exist
        let res = client.delete_kv("not exists", None).await?;
        assert!(res.is_none());

        // do not care seq
        client
            .upsert_kv(test_key, None, "value of ak".to_string().into_bytes())
            .await?;

        let res = client.delete_kv(test_key, None).await?;
        assert!(res.is_some());
    }

    // update
    {
        let test_key = "test_key_for_update";
        let r = client
            .update_kv(test_key, None, "value of ak".to_string().into_bytes())
            .await?;
        assert!(r.is_none());

        let r = client
            .upsert_kv(test_key, None, "value of ak".to_string().into_bytes())
            .await?;
        assert!(r.result.is_some());
        let seq = r.result.unwrap().0;

        // unmatched seq
        let r = client
            .update_kv(
                test_key,
                Some(seq + 1),
                "value of ak".to_string().into_bytes(),
            )
            .await?;
        assert!(r.is_none());

        // matched seq
        let r = client
            .update_kv(test_key, Some(seq), "value of ak".to_string().into_bytes())
            .await?;
        assert!(r.is_some());

        // blind update
        let r = client
            .update_kv(test_key, None, "brand new value".to_string().into_bytes())
            .await?;
        assert!(r.is_some());

        // value updated
        let kv = client.get_kv(test_key).await?;
        assert!(kv.result.is_some());
        assert_eq!(kv.result.unwrap().1, "brand new value".as_bytes());
    }

    Ok(())
}
