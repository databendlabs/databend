use std::sync::Arc;

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_expression::TableSchema;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

pub(super) fn naive_parquet_table_info(schema: Arc<TableSchema>) -> TableInfo {
    TableInfo {
        ident: TableIdent::new(0, 0),
        desc: "''.'read_parquet'".to_string(),
        name: "read_parquet".to_string(),
        meta: TableMeta {
            schema,
            engine: "SystemReadParquet".to_string(),
            created_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
            updated_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
            ..Default::default()
        },
        ..Default::default()
    }
}
