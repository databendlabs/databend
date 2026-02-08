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

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FormatTreeNode;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::StringType;
use databend_common_pipeline::core::SharedLockGuard;
use databend_meta_types::MetaId;

use super::insert::format_insert_source;
use crate::FormatOptions;
use crate::plans::InsertInputSource;

#[derive(Clone)]
pub struct Replace {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub table_id: MetaId,
    pub on_conflict_fields: Vec<TableField>,
    pub schema: TableSchemaRef,
    pub source: InsertInputSource,
    pub delete_when: Option<Expr>,
    pub lock_guard: Option<SharedLockGuard>,
}

impl PartialEq for Replace {
    fn eq(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.database == other.database
            && self.table == other.table
            && self.schema == other.schema
            && self.on_conflict_fields == other.on_conflict_fields
    }
}

impl Replace {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(self.schema.clone().into())
    }

    pub fn has_select_plan(&self) -> bool {
        matches!(&self.source, InsertInputSource::SelectPlan(_))
    }

    #[async_backtrace::framed]
    pub async fn explain(
        &self,
        options: FormatOptions,
    ) -> databend_common_exception::Result<Vec<DataBlock>> {
        let mut result = vec![];

        let Replace {
            catalog,
            database,
            table,
            source,
            on_conflict_fields,
            ..
        } = self;

        let table_name = format!("{}.{}.{}", catalog, database, table);
        let on_columns = on_conflict_fields
            .iter()
            .map(|field| format!("{}.{} (#{})", table, field.name, field.column_id))
            .collect::<Vec<_>>()
            .join(",");

        let children = vec![
            FormatTreeNode::new(format!("table: {table_name}")),
            FormatTreeNode::new(format!("on columns: [{on_columns}]")),
        ];

        let formatted_plan = format_insert_source("ReplacePlan", source, options, children)?;
        let line_split_result: Vec<&str> = formatted_plan.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        result.push(DataBlock::new_from_columns(vec![formatted_plan]));
        Ok(vec![DataBlock::concat(&result)?])
    }
}

impl std::fmt::Debug for Replace {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Replace")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("table_id", &self.table_id)
            .field("schema", &self.schema)
            .field("on conflict", &self.on_conflict_fields)
            .finish()
    }
}
