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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::utils::ColumnFrom;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::rdoc::FunctionDocAsset;
use common_functions::rdoc::FunctionDocs;
use common_functions::scalars::FunctionFactory;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_types::UserDefinedFunction;
use common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct FunctionsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for FunctionsTable {
    const NAME: &'static str = "system.functions";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<Chunk> {
        let function_factory = FunctionFactory::instance();
        let aggregate_function_factory = AggregateFunctionFactory::instance();
        let func_names = function_factory.registered_names();
        let aggr_func_names = aggregate_function_factory.registered_names();
        let udfs = FunctionsTable::get_udfs(ctx).await?;

        let names: Vec<&str> = func_names
            .iter()
            .chain(aggr_func_names.iter())
            .chain(udfs.iter().map(|udf| &udf.name))
            .map(|x| x.as_str())
            .collect();

        let builtin_func_len = func_names.len() + aggr_func_names.len();

        let docs = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    let name = &names[i];
                    FunctionDocAsset::get_doc(name)
                } else {
                    FunctionDocs::default()
                }
            })
            .collect::<Vec<_>>();

        let is_builtin = (0..names.len())
            .map(|i| i < builtin_func_len)
            .collect::<Vec<bool>>();

        let is_aggregate = (0..names.len())
            .map(|i| i >= func_names.len() && i < builtin_func_len)
            .collect::<Vec<bool>>();

        let definitions = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    ""
                } else {
                    udfs.get(i - builtin_func_len)
                        .map_or("", |udf| udf.definition.as_str())
                }
            })
            .collect::<Vec<&str>>();

        let categorys = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    docs[i].category.as_str()
                } else {
                    "UDF"
                }
            })
            .collect::<Vec<&str>>();

        let descriptions = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    docs[i].description.as_str()
                } else {
                    udfs.get(i - builtin_func_len)
                        .map_or("", |udf| udf.description.as_str())
                }
            })
            .collect::<Vec<&str>>();

        let syntaxs = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    docs[i].syntax.as_str()
                } else {
                    udfs.get(i - builtin_func_len)
                        .map_or("", |udf| udf.definition.as_str())
                }
            })
            .collect::<Vec<&str>>();

        let examples = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    docs[i].example.as_str()
                } else {
                    ""
                }
            })
            .collect::<Vec<&str>>();

        let rows_len = func_names.len();
        Ok(Chunk::new_from_sequence(
            vec![
                (Value::Column(Column::from_data(names)), DataType::String),
                (
                    Value::Column(Column::from_data(is_builtin)),
                    DataType::Boolean,
                ),
                (
                    Value::Column(Column::from_data(is_aggregate)),
                    DataType::Boolean,
                ),
                (
                    Value::Column(Column::from_data(definitions)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(categorys)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(descriptions)),
                    DataType::String,
                ),
                (Value::Column(Column::from_data(syntaxs)), DataType::String),
                (Value::Column(Column::from_data(examples)), DataType::String),
            ],
            rows_len,
        ))
    }
}

impl FunctionsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("is_builtin", TableDataType::Boolean),
            TableField::new("is_aggregate", TableDataType::Boolean),
            TableField::new("definition", TableDataType::String),
            TableField::new("category", TableDataType::String),
            TableField::new("description", TableDataType::String),
            TableField::new("syntax", TableDataType::String),
            TableField::new("example", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'functions'".to_string(),
            name: "functions".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemFunctions".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(FunctionsTable { table_info })
    }

    async fn get_udfs(ctx: Arc<dyn TableContext>) -> Result<Vec<UserDefinedFunction>> {
        let tenant = ctx.get_tenant();
        UserApiProvider::instance().get_udfs(&tenant).await
    }
}
