// Copyright 2021 Datafuse Labs.
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

use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::utils::ColumnFrom;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::SchemaDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct ColumnsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for ColumnsTable {
    const NAME: &'static str = "system.columns";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<Chunk> {
        let rows = self.dump_table_columns(ctx).await?;
        let mut names: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut tables: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut databases: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut data_types: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut default_kinds: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut default_exprs: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut is_nullables: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut comments: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let rows_len = rows.len();
        for (database_name, table_name, field) in rows.into_iter() {
            names.push(field.name().clone().into_bytes());
            tables.push(table_name.into_bytes());
            databases.push(database_name.into_bytes());

            let data_type = field.data_type().sql_name();
            data_types.push(data_type.into_bytes());

            let mut default_kind = "".to_string();
            let mut default_expr = "".to_string();
            if let Some(expr) = field.default_expr() {
                default_kind = "DEFAULT".to_string();
                default_expr = expr.to_string();
            }
            default_kinds.push(default_kind.into_bytes());
            default_exprs.push(default_expr.into_bytes());
            if field.is_nullable() {
                is_nullables.push("YES".to_string().into_bytes());
            } else {
                is_nullables.push("NO".to_string().into_bytes());
            }

            comments.push("".to_string().into_bytes());
        }

        Ok(Chunk::new_from_sequence(
            vec![
                (Value::Column(Column::from_data(names)), DataType::String),
                (
                    Value::Column(Column::from_data(databases)),
                    DataType::String,
                ),
                (Value::Column(Column::from_data(tables)), DataType::String),
                (
                    Value::Column(Column::from_data(data_types)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(default_kinds)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(default_exprs)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(is_nullables)),
                    DataType::String,
                ),
                (Value::Column(Column::from_data(comments)), DataType::String),
            ],
            rows_len,
        ))
    }
}

impl ColumnsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", SchemaDataType::String),
            TableField::new("database", SchemaDataType::String),
            TableField::new("table", SchemaDataType::String),
            TableField::new("type", SchemaDataType::String),
            TableField::new("default_kind", SchemaDataType::String),
            TableField::new("default_expression", SchemaDataType::String),
            TableField::new("is_nullable", SchemaDataType::String),
            TableField::new("comment", SchemaDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'columns'".to_string(),
            name: "columns".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemColumns".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(ColumnsTable { table_info })
    }

    async fn dump_table_columns(
        &self,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Vec<(String, String, TableField)>> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(CATALOG_DEFAULT)?;
        let databases = catalog.list_databases(tenant.as_str()).await?;

        let mut rows: Vec<(String, String, TableField)> = vec![];
        for database in databases {
            for table in catalog
                .list_tables(tenant.as_str(), database.name())
                .await?
            {
                for field in table.schema().fields() {
                    rows.push((database.name().into(), table.name().into(), field.clone()))
                }
            }
        }

        Ok(rows)
    }
}
