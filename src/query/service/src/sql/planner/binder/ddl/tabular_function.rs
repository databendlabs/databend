// Copyright 2022 Datafuse Labs.
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

use common_ast::ast::CreateTabularFunctionStmt;
use common_ast::UDFValidator;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::TypeFactory;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::normalize_identifier;
use crate::sql::plans::CreateTabularFunctionPlan;
use crate::sql::plans::Plan;
use crate::sql::BindContext;
use crate::sql::Binder;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_create_tabular_function(
        &mut self,
        stmt: &CreateTabularFunctionStmt<'a>,
    ) -> Result<Plan> {
        let CreateTabularFunctionStmt {
            if_not_exists,
            name,
            args,
            source,
            as_query,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let func_name = normalize_identifier(name, &self.name_resolution_ctx).name;

        let mut source_fields: Vec<DataField> = Vec::with_capacity(source.len());
        for column in source.iter() {
            let name = normalize_identifier(&column.name, &self.name_resolution_ctx).name;
            let data_type = TypeFactory::instance().get(column.data_type.to_string())?;

            source_fields.push(DataField::new(&name, data_type.clone()));
        }

        let arg_fields: Option<Vec<DataField>> = match args {
            Some(v) => {
                let mut fields: Vec<DataField> = Vec::with_capacity(source.len());
                for column in v.iter() {
                    let name = normalize_identifier(&column.name, &self.name_resolution_ctx).name;
                    let data_type = TypeFactory::instance().get(column.data_type.to_string())?;

                    fields.push(DataField::new(&name, data_type.clone()));
                }
                Some(fields)
            }
            None => None,
        };

        let init_bind_context = BindContext::new();
        let (_s_expr, bind_context) = self.bind_query(&init_bind_context, as_query).await?;
        let query_fields: Vec<DataField> = bind_context
            .columns
            .iter()
            .map(|column_binding| {
                DataField::new(
                    &column_binding.column_name,
                    *column_binding.data_type.clone(),
                )
            })
            .collect();

        if source_fields.len() != query_fields.len() {
            return Err(ErrorCode::SyntaxException(format!(
                "table column len {} not match query column len {}",
                source_fields.len(),
                query_fields.len()
            )));
        }

        let mut validator = UDFValidator {
            name: func_name.clone(),
            parameters: arg_fields.as_ref().map_or(vec![], |v| {
                let params: Vec<String> = v.iter().map(|col| col.name().to_string()).collect();
                params
            }),
            ..Default::default()
        };
        validator.verify_definition_query(as_query)?;
        let query = format!("{}", as_query);
        let plan = CreateTabularFunctionPlan {
            if_not_exists: *if_not_exists,
            tenant,
            name: func_name.clone(),
            args: arg_fields.map(|v| DataSchemaRefExt::create(v)),
            source: DataSchemaRefExt::create(source_fields),
            as_query: query,
        };
        Ok(Plan::CreateTabularFunction(Box::new(plan)))
    }
}
