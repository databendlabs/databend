// Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_meta_app::share::ShareGrantObjectPrivilege;
use databend_sharing_endpoint::models::DatabaseSpec;
use databend_sharing_endpoint::models::LambdaInput;
use databend_sharing_endpoint::models::ShareSpec;
use databend_sharing_endpoint::models::SharingConfig;
use databend_sharing_endpoint::models::TableSpec;
// mock some SharingConfig
// and test on SharingConfig get_tables method
#[tokio::test(flavor = "multi_thread")]
async fn test_get_tables() -> Result<()> {
    let mut config = SharingConfig {
        share_specs: HashMap::new(),
    };
    config.share_specs.insert("share1".to_string(), ShareSpec {
        name: "share1".to_string(),
        share_id: 0,
        version: 0,
        database: Some(DatabaseSpec {
            name: "db1".to_string(),
            location: "s3://db1".to_string(),
            id: 0,
        }),
        tables: vec![
            TableSpec {
                name: "table1".to_string(),
                location: "s3://db1/table1".to_string(),
                database_id: 0,
                table_id: 0,
                presigned_url_timeout: "".to_string(),
            },
            TableSpec {
                name: "table2".to_string(),
                location: "s3://db1/table2".to_string(),
                database_id: 0,
                table_id: 1,
                presigned_url_timeout: "".to_string(),
            },
        ],
        tenants: vec!["t1".to_string()],
        db_privileges: Some(ShareGrantObjectPrivilege::Usage.into()),
        comment: None,
        share_on: None,
    });

    let input = LambdaInput {
        authorization: "".to_string(),
        tenant_id: "t1".to_string(),
        share_name: "share1".to_string(),
        table_name: "table1".to_string(),
        request_files: vec![],
        request_id: "123".to_string(),
    };
    let table = config.get_tables(&input)?;
    assert!(table.is_some());
    assert_eq!(table.as_ref().unwrap().table, "table1");
    assert_eq!(table.as_ref().unwrap().location, "s3://db1/table1");

    Ok(())
}
