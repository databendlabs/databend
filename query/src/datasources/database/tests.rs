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

use common_base::tokio;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::*;
use pretty_assertions::assert_eq;

use crate::catalogs::Catalog;
use crate::tests::try_create_catalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource() -> Result<()> {
    let catalog = try_create_catalog()?;

    // Table check.
    let tbl_arg = Some(vec![Expression::create_literal(DataValue::Int64(Some(1)))]);
    catalog.get_table_function("numbers_mt", tbl_arg)?;
    if let Err(e) = catalog.get_table("system", "numbersxx").await {
        let expect = "Code: 25, displayText = Unknown table: \'numbersxx\'.";
        let actual = format!("{}", e);
        assert_eq!(expect, actual);
    }

    // Database tests.
    {
        // Create database.
        catalog
            .create_database(CreateDatabasePlan {
                if_not_exists: false,
                db: "test_db".to_string(),
                options: Default::default(),
            })
            .await?;

        // Check
        let result = catalog.get_database("test_db").await;
        assert!(result.is_ok());

        // Drop database.
        catalog
            .drop_database(DropDatabasePlan {
                if_exists: false,
                db: "test_db".to_string(),
            })
            .await?;

        // Check.
        let result = catalog.get_database("test_db").await;
        assert!(result.is_err());
    }

    Ok(())
}
