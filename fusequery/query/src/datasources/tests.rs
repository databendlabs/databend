// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource() -> anyhow::Result<()> {
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::datasources::IDataSource;
    use crate::datasources::*;

    let datasource = DataSource::try_create()?;

    // Table check.
    datasource.get_table("system", "numbers_mt")?;
    if let Err(e) = datasource.get_table("system", "numbersxx") {
        let expect = "Code: 25, displayText = Unknown table: \'numbersxx\'.";
        let actual = format!("{}", e);
        assert_eq!(expect, actual);
    }

    // Database tests.
    {
        // Create database.
        datasource
            .create_database(CreateDatabasePlan {
                if_not_exists: false,
                db: "test_db".to_string(),
                engine: DatabaseEngineType::Local,
                options: Default::default()
            })
            .await?;

        // Check
        let result = datasource.get_database("test_db");
        assert_eq!(true, result.is_ok());

        // Drop database.
        datasource
            .drop_database(DropDatabasePlan {
                if_exists: false,
                db: "test_db".to_string()
            })
            .await?;

        // Check.
        let result = datasource.get_database("test_db");
        assert_eq!(true, result.is_err());
    }

    Ok(())
}
