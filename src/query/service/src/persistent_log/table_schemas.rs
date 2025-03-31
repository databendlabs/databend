use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;

pub trait PersistentLogTable: Send + Sync + 'static {
    fn table_name(&self) -> &'static str;
    fn schema(&self) -> TableSchemaRef;
    fn cluster_by(&self) -> Vec<String> {
        vec![]
    }
    fn create_table_sql(&self) -> String {
        let table_name = self.table_name();
        let schema = self.schema();
        let fields = schema
            .fields()
            .iter()
            .map(|f| format!("{} {}", f.name(), f.data_type().sql_name()))
            .collect::<Vec<_>>()
            .join(", ");
        let cluster_by = self.cluster_by().join(", ");
        format!(
            "CREATE TABLE IF NOT EXISTS persistent_system.{} ({}) CLUSTER BY ({})",
            table_name, fields, cluster_by
        )
    }
    fn schema_equal(&self, other: TableSchemaRef) -> bool {
        self.schema().fields().len() == other.fields().len()
            && self
                .schema()
                .fields()
                .iter()
                .zip(other.fields().iter())
                .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
    }
}

pub struct QueryLogTable;

impl PersistentLogTable for QueryLogTable {
    fn table_name(&self) -> &'static str {
        "query_log"
    }
    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new(
                "timestamp",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new(
                "path",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "target",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "log_level",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "cluster_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "node_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "warehouse_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "query_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "message",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "fields",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
        ])
    }
    fn cluster_by(&self) -> Vec<String> {
        vec!["timestamp".to_string(), "query_id".to_string()]
    }
}
