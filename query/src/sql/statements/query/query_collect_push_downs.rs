use std::collections::HashSet;
use common_planners::{Expression, Extras};
use crate::sql::statements::query::query_ast_ir::QueryASTIRVisitor;
use crate::sql::statements::QueryASTIR;
use common_exception::Result;
use crate::sql::statements::query::{JoinedSchema, JoinedTableDesc};

pub struct QueryCollectPushDowns {
    require_columns: HashSet<String>,
}

///
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
}

impl QueryCollectPushDowns {
    pub fn collect_extras(ir: &mut QueryASTIR, schema: &mut JoinedSchema) -> Result<()> {
        let mut push_downs_data = Self { require_columns: HashSet::new() };
        QueryCollectPushDowns::visit(ir, &mut push_downs_data)?;
        push_downs_data.collect_push_downs(schema)
    }

    fn collect_push_downs(mut self, schema: &mut JoinedSchema) -> Result<()> {
        for index in 0..schema.get_tables_desc().len() {
            let table_desc = &schema.get_tables_desc()[index];
            let projection = self.collect_table_require_columns(table_desc);

            if !projection.is_empty() {
                schema.set_table_push_downs(index, Extras {
                    projection: Some(projection),
                    // TODO:
                    filters: vec![],
                    limit: None,
                    order_by: vec![],
                });
            }
        }

        Ok(())
    }

    fn collect_table_require_columns(&mut self, table_desc: &JoinedTableDesc) -> Vec<usize> {
        let mut table_require_columns = Vec::new();

        let columns_desc = table_desc.get_columns_desc();
        for column_index in 0..columns_desc.len() {
            let column_desc = &columns_desc[column_index];

            let column_name = match column_desc.is_ambiguity {
                true => format!("{}.{}", table_desc.get_name_parts().join("."), column_desc.short_name),
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

