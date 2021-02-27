// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource() -> crate::error::FuseQueryResult<()> {
    use crate::datasources::*;

    let mut datasource = DataSource::try_create()?;

    // Database check.
    let actual = format!("{:?}", datasource.check_database("xx"));
    let expect = "Err(Internal(\"Unknown database: \\'xx\\'\"))";
    assert_eq!(expect, actual);

    // Table check.
    datasource.get_table("system", "numbers_mt")?;
    if let Err(e) = datasource.get_table("system", "numbersxx") {
        let expect = "Internal(\"Unknown table: \\'system.numbersxx\\'\")";
        let actual = format!("{:?}", e);
        assert_eq!(expect, actual);
    }
    Ok(())
}
