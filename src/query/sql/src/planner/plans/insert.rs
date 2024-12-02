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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableInfo;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use super::Plan;

#[derive(Clone, Debug, EnumAsInner)]
pub enum InsertInputSource {
    SelectPlan(Box<Plan>),
    Values(InsertValue),
    // From stage
    Stage(Box<Plan>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InsertValue {
    // Scalars of literal values
    Values { rows: Vec<Vec<Scalar>> },
    // Raw values of clickhouse dialect
    RawValues { data: String, start: usize },
}

#[derive(Clone)]
pub struct Insert {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub schema: TableSchemaRef,
    pub overwrite: bool,
    pub source: InsertInputSource,
    // if a table with fixed table id, and version should be used,
    // it should be provided as some `table_info`.
    // otherwise, the table being inserted will be resolved by using `catalog`.`database`.`table`
    pub table_info: Option<TableInfo>,
}

impl PartialEq for Insert {
    fn eq(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.database == other.database
            && self.table == other.table
            && self.schema == other.schema
    }
}

impl Insert {
    pub fn dest_schema(&self) -> DataSchemaRef {
        Arc::new(self.schema.clone().into())
    }

    pub fn has_select_plan(&self) -> bool {
        matches!(&self.source, InsertInputSource::SelectPlan(_))
    }

    #[async_backtrace::framed]
    pub async fn explain(
        &self,
        verbose: bool,
    ) -> databend_common_exception::Result<Vec<DataBlock>> {
        let mut result = vec![];

        let Insert {
            catalog,
            database,
            table,
            schema,
            overwrite,
            // table_info only used create table as select.
            table_info: _,
            source,
        } = self;

        let table_name = format!("{}.{}.{}", catalog, database, table);
        let inserted_columns = schema
            .fields
            .iter()
            .map(|field| format!("{}.{} (#{})", table, field.name, field.column_id))
            .collect::<Vec<_>>()
            .join(",");

        let children = vec![
            FormatTreeNode::new(format!("table: {table_name}")),
            FormatTreeNode::new(format!("inserted columns: [{inserted_columns}]")),
            FormatTreeNode::new(format!("overwrite: {overwrite}")),
        ];

        let formatted_plan = format_insert_source("InsertPlan", source, verbose, children)?;

        let line_split_result: Vec<&str> = formatted_plan.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        result.push(DataBlock::new_from_columns(vec![formatted_plan]));
        Ok(vec![DataBlock::concat(&result)?])
    }
}

pub(crate) fn format_insert_source(
    plan_name: &str,
    source: &InsertInputSource,
    verbose: bool,
    mut children: Vec<FormatTreeNode>,
) -> databend_common_exception::Result<String> {
    match source {
        InsertInputSource::SelectPlan(plan) => {
            if let Plan::Query {
                s_expr, metadata, ..
            } = &**plan
            {
                let metadata = &*metadata.read();
                let sub_tree = s_expr.to_format_tree(metadata, verbose)?;
                children.push(sub_tree);

                return Ok(FormatTreeNode::with_children(
                    format!("{plan_name} (subquery):"),
                    children,
                )
                .format_pretty()?);
            }
            Ok(String::new())
        }
        InsertInputSource::Values(values) => match values {
            InsertValue::Values { .. } => Ok(FormatTreeNode::with_children(
                format!("{plan_name} (values):"),
                children,
            )
            .format_pretty()?),
            InsertValue::RawValues { .. } => Ok(FormatTreeNode::with_children(
                format!("{plan_name} (rawvalues):"),
                children,
            )
            .format_pretty()?),
        },
        InsertInputSource::Stage(_) => Err(ErrorCode::StorageUnsupported(
            "stage attachment is deprecated in replace into statement",
        )),
    }
}

impl std::fmt::Debug for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Insert")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("schema", &self.schema)
            .field("overwrite", &self.overwrite)
            .finish()
    }
}
