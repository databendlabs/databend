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

use std::collections::HashSet;

use common_datavalues::DataType;
use common_exception::Result;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::Projection;

use crate::sql::statements::query::query_ast_ir::QueryASTIRVisitor;
use crate::sql::statements::query::JoinedSchema;
use crate::sql::statements::query::JoinedTableDesc;
use crate::sql::statements::QueryASTIR;

pub struct QueryCollectPushDowns {
    require_columns: HashSet<String>,
    require_filters: Vec<Expression>,
    aggregating: bool,
}

/// Collect the query need to push downs parts .
impl QueryASTIRVisitor<QueryCollectPushDowns> for QueryCollectPushDowns {
    fn visit_expr(expr: &mut Expression, data: &mut QueryCollectPushDowns) -> Result<()> {
        if let Expression::Column(name) = expr {
            let require_columns = &mut data.require_columns;

            if !require_columns.contains(name) {
                require_columns.insert(name.clone());
            }
        }

        Ok(())
    }

    fn visit_filter(predicate: &mut Expression, data: &mut QueryCollectPushDowns) -> Result<()> {
        data.require_filters = vec![predicate.clone()];
        Self::visit_recursive_expr(predicate, data)
    }
}

impl QueryCollectPushDowns {
    pub fn collect_extras(
        ir: &mut QueryASTIR,
        schema: &mut JoinedSchema,
        aggregating: bool,
    ) -> Result<()> {
        let mut push_downs_data = Self {
            require_columns: HashSet::new(),
            require_filters: vec![],
            aggregating,
        };
        QueryCollectPushDowns::visit(ir, &mut push_downs_data)?;
        push_downs_data.collect_push_downs(ir, schema)
    }

    fn collect_push_downs(mut self, ir: &QueryASTIR, schema: &mut JoinedSchema) -> Result<()> {
        for index in 0..schema.get_tables_desc().len() {
            let table_desc = &schema.get_tables_desc()[index];
            let col_indices = self.collect_table_require_columns(table_desc);
            let projection = Projection::Columns(col_indices);

            let mut limit = None;
            let mut order_by = vec![];
            if schema.get_tables_desc().len() == 1
                && ir.group_by_expressions.is_empty()
                && ir.aggregate_expressions.is_empty()
            {
                limit = ir.limit.map(|c| c + ir.offset.unwrap_or(0));
                order_by = ir.order_by_expressions.clone();
            }

            schema.set_table_push_downs(index, Extras {
                projection: Some(projection),
                filters: self.require_filters.clone(),
                prewhere: None, // This mod is no more used, so we don't need consider this.
                limit,
                order_by,
            });
        }

        Ok(())
    }

    fn collect_table_require_columns(&mut self, table_desc: &JoinedTableDesc) -> Vec<usize> {
        let has_exact_total_row_count = if let JoinedTableDesc::Table { table, .. } = table_desc {
            table.has_exact_total_row_count()
        } else {
            // subquery not handled yet
            false
        };

        match self.require_columns.is_empty() {
            true if self.aggregating && has_exact_total_row_count => {
                // This query
                // - has aggregation expression in "projection"
                // - requires no columns
                // - DO have the exact number of row count
                // thus, no need to collect the smallest column
                vec![]
            }
            true => Self::collect_table_smallest_column(table_desc),
            false => self.collect_table_projection_columns(table_desc),
        }
    }

    // SELECT COUNT() FROM table_name.
    fn collect_table_smallest_column(table_desc: &JoinedTableDesc) -> Vec<usize> {
        let mut smallest_index = 0;
        let mut smallest_size = usize::MAX;
        let columns_desc = table_desc.get_columns_desc();
        for (column_index, column_desc) in columns_desc.iter().enumerate() {
            if let Ok(bytes) = column_desc.data_type.data_type_id().numeric_byte_size() {
                if smallest_size > bytes {
                    smallest_size = bytes;
                    smallest_index = column_index;
                }
            }
        }

        vec![smallest_index]
    }

    fn collect_table_projection_columns(&mut self, table_desc: &JoinedTableDesc) -> Vec<usize> {
        let mut table_require_columns = Vec::new();
        let columns_desc = table_desc.get_columns_desc();
        for (column_index, column_desc) in columns_desc.iter().enumerate() {
            let column_name = match column_desc.is_ambiguity {
                true => format!(
                    "{}.{}",
                    table_desc.get_name_parts().join("."),
                    column_desc.short_name
                ),
                false => column_desc.short_name.clone(),
            };

            if self.require_columns.remove(&column_name) {
                // Require this column.
                table_require_columns.push(column_index);
            }
        }

        table_require_columns
    }
}
