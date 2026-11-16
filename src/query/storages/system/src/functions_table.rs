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

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::utils::FromData;
use databend_common_functions::ASYNC_FUNCTIONS;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::TypeChecker;

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

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        _: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let mut scalar_func_names: Vec<String> = BUILTIN_FUNCTIONS.registered_names();
        scalar_func_names.extend(
            TypeChecker::<()>::all_special_functions()
                .iter()
                .map(|name| name.to_string()),
        );
        scalar_func_names.extend(
            TypeChecker::<()>::all_rewrite_functions()
                .iter()
                .map(|name| name.to_string()),
        );
        scalar_func_names.extend(
            databend_common_ast::ast::Expr::all_function_like_syntaxes()
                .iter()
                .map(|name| name.to_lowercase()),
        );
        scalar_func_names.extend(
            ASYNC_FUNCTIONS
                .iter()
                .map(|name| name.into_inner().to_string()),
        );
        scalar_func_names.sort();

        // Combinator-derived names such as `avg_if` or `avg_state` are withheld:
        // their documentation describes the base name they were derived from.
        // Aliases are kept, matching how the scalar registry reports its own names;
        // an alias resolves to its target's documentation.
        let published_aggregates = AGGR_REGISTRY
            .registered_names()
            .into_iter()
            .filter_map(|name| {
                let features = AGGR_REGISTRY.descriptor(&name)?.features();
                (!features.hide_doc).then_some((name, features))
            })
            .collect::<Vec<_>>();

        let num_rows = scalar_func_names.len() + published_aggregates.len();
        let mut names = StringColumnBuilder::with_capacity(num_rows);
        let mut is_aggregate = Vec::with_capacity(num_rows);
        let mut descriptions = StringColumnBuilder::with_capacity(num_rows);
        let mut syntaxes = StringColumnBuilder::with_capacity(num_rows);
        let mut examples = StringColumnBuilder::with_capacity(num_rows);

        // Scalar functions carry no documentation in the function registry, so
        // their documentation rows stay empty.
        for name in &scalar_func_names {
            names.put_and_commit(name);
            is_aggregate.push(false);
            descriptions.commit_row();
            syntaxes.commit_row();
            examples.commit_row();
        }

        for (name, features) in &published_aggregates {
            names.put_and_commit(name);
            is_aggregate.push(true);
            descriptions.put_and_commit(features.description);
            syntaxes.put_and_commit(features.definition);
            examples.put_and_commit(features.example);
        }

        Ok(DataBlock::new_from_columns(vec![
            Column::String(names.build()),
            BooleanType::from_data(is_aggregate),
            Column::String(descriptions.build()),
            Column::String(syntaxes.build()),
            Column::String(examples.build()),
        ]))
    }
}

impl FunctionsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("is_aggregate", TableDataType::Boolean),
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
}
