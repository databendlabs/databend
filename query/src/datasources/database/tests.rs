// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::catalogs::Catalog;
use crate::tests::try_create_catalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource() -> Result<()> {
    let catalog = try_create_catalog()?;

    // Table check.
    catalog.get_table("system", "numbers_mt")?;
    if let Err(e) = catalog.get_table("system", "numbersxx") {
        let expect = "Code: 25, displayText = Unknown table: \'numbersxx\'.";
        let actual = format!("{}", e);
        assert_eq!(expect, actual);
    }

    // Database tests.
    {
        // Create database.
        catalog.create_database(CreateDatabasePlan {
            if_not_exists: false,
            db: "test_db".to_string(),
            engine: "default".to_string(),
            options: Default::default(),
        })?;

        // Check
        let result = catalog.get_database("test_db");
        assert_eq!(true, result.is_ok());

        // Drop database.
        catalog.drop_database(DropDatabasePlan {
            if_exists: false,
            db: "test_db".to_string(),
        })?;

        // Check.
        let result = catalog.get_database("test_db");
        assert_eq!(true, result.is_err());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource_invalid_db_engine() -> Result<()> {
    let catalog = try_create_catalog()?;

    // Create database.
    let r = catalog.create_database(CreateDatabasePlan {
        if_not_exists: false,
        db: "test_db".to_string(),
        engine: "Local".to_string(),
        options: Default::default(),
    });
    assert_eq!(true, r.is_err());
    let err = r.unwrap_err();
    assert_eq!(err.code(), ErrorCode::UnknownDatabaseEngine("").code());

    Ok(())
}
