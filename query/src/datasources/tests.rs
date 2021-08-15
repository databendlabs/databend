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

use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::catalogs::catalog::Catalog;
use crate::datasources::DatabaseCatalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource() -> Result<()> {
    let datasource = DatabaseCatalog::try_create()?;

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
                options: Default::default(),
            })
            .await?;

        // Check
        let result = datasource.get_database("test_db");
        assert_eq!(true, result.is_ok());

        // Drop database.
        datasource
            .drop_database(DropDatabasePlan {
                if_exists: false,
                db: "test_db".to_string(),
            })
            .await?;

        // Check.
        let result = datasource.get_database("test_db");
        assert_eq!(true, result.is_err());
    }

    Ok(())
}
