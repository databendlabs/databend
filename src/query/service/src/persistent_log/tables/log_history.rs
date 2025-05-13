// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;

use crate::persistent_log::tables::HistoryTable;

/// This table stores the system log extracted from raw parquet log files.
/// It serves as the primary source of information for other tables,
/// which extract only the specific data they require.
pub fn log_history() -> Arc<HistoryTable> {
    Arc::new(HistoryTable {
        name: String::from("log_history"),
        schema: TableSchemaRefExt::create(vec![
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
            TableField::new("batch_number", TableDataType::Number(NumberDataType::Int64)),
        ]),
        cluster_by: vec!["batch_number".to_string()],
        transform_sql: String::from(
            "COPY INTO system_history.log_history FROM (
                    SELECT timestamp, path, target, log_level, cluster_id,
                    node_id, warehouse_id, query_id, message, fields, {batch_number}
                    FROM @{stage_name}
                 ) file_format = (TYPE = PARQUET) PURGE = TRUE",
        ),
    })
}
