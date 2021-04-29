// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::datasources::*;

    let datasource = DataSource::try_create()?;

    // Table check.
    datasource.get_table("system", "numbers_mt")?;
    if let Err(e) = datasource.get_table("system", "numbersxx") {
        let expect = "Code: 25, displayText = DataSource Error: Unknown table: \'numbersxx\'.";
        let actual = format!("{}", e);
        assert_eq!(expect, actual);
    }
    Ok(())
}
