// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource() -> crate::error::FuseQueryResult<()> {
    use crate::testdata;

    let datasource = testdata::NumberTestData::create().number_source_for_test()?;
    datasource.lock()?.get_table("system", "numbers_mt")?;
    if let Err(e) = datasource.lock()?.get_table("system", "numbersxx") {
        let expect = "Internal(\"Cannot find the table: numbersxx\")";
        let actual = format!("{:?}", e);
        assert_eq!(expect, actual);
    }
    Ok(())
}
