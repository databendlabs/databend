use std::collections::HashMap;

use common_base::base::tokio;
use common_exception::Result;
use sharing_endpoint::models::DatabaseSpec;
use sharing_endpoint::models::LambdaInput;
use sharing_endpoint::models::ShareSpec;
use sharing_endpoint::models::SharingConfig;
use sharing_endpoint::models::TableSpec;
// mock some SharingConfig
// and test on SharingConfig get_tables method
#[tokio::test]
async fn test_get_tables() -> Result<()> {
    let mut config = SharingConfig {
        share_specs: HashMap::new(),
    };
    config.share_specs.insert("share1".to_string(), ShareSpec {
        name: "share1".to_string(),
        share_id: 0,
        version: 0,
        database: DatabaseSpec {
            name: "db1".to_string(),
            location: "s3://db1".to_string(),
            id: 0,
        },
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
