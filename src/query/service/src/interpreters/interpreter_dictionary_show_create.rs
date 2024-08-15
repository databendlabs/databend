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

use databend_common_ast::ast::quote::display_ident;
use databend_common_ast::parser::Dialect;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::Value;
use databend_common_meta_app::schema::tenant_dictionary_ident::TenantDictionaryIdent;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_sql::plans::ShowCreateDictionaryPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ShowCreateDictionaryInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowCreateDictionaryPlan,
}

pub struct ShowCreateQuerySettings {
    pub sql_dialect: Dialect,
    pub quoted_ident_case_sensitive: bool,
}

impl ShowCreateDictionaryInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowCreateDictionaryPlan) -> Result<Self> {
        Ok(ShowCreateDictionaryInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowCreateDictionaryInterpreter {
    fn name(&self) -> &str {
        "ShowCreateDictionaryInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        let dict_name = self.plan.dictionary.clone();

        let dict_ident = TenantDictionaryIdent::new(
            tenant,
            DictionaryIdentity::new(self.plan.database_id, dict_name.clone()),
        );
        let dictionary = if let Some(reply) = catalog.get_dictionary(dict_ident).await? {
            reply.dictionary_meta
        } else {
            return Ok(PipelineBuildResult::create());
        };
        let settings = self.ctx.get_settings();
        let settings = ShowCreateQuerySettings {
            sql_dialect: settings.get_sql_dialect()?,
            quoted_ident_case_sensitive: settings.get_quoted_ident_case_sensitive()?,
        };

        let create_query: String =
            Self::show_create_query(catalog.as_ref(), &dictionary, &dict_name, &settings).await?;
        let block = DataBlock::new(
            vec![
                BlockEntry::new(DataType::String, Value::Scalar(Scalar::String(dict_name))),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(create_query)),
                ),
            ],
            1,
        );
        PipelineBuildResult::from_blocks(vec![block])
    }
}

impl ShowCreateDictionaryInterpreter {
    pub async fn show_create_query(
        _catalog: &dyn Catalog,
        dictionary: &DictionaryMeta,
        dict_name: &str,
        settings: &ShowCreateQuerySettings,
    ) -> Result<String> {
        let sql_dialect = settings.sql_dialect;
        let quoted_ident_case_sensitive = settings.quoted_ident_case_sensitive;
        let schema = dictionary.schema.clone();
        let n_fields = schema.fields().len();
        let source = dictionary.source.clone();
        let source_options = dictionary.options.clone();
        let comment = dictionary.comment.clone();
        let pk_id_list = dictionary.primary_column_ids.clone();
        let field_comments = dictionary.field_comments.clone();

        let mut dict_create_sql = format!(
            "CREATE DICTIONARY {} (\n",
            display_ident(dict_name, quoted_ident_case_sensitive, sql_dialect)
        );

        // Append columns and indexes.
        {
            let mut create_defs = vec![];
            for (idx, field) in schema.fields().iter().enumerate() {
                let nullable = if field.is_nullable() {
                    " NULL".to_string()
                } else {
                    " NOT NULL".to_string()
                };
                let default_expr = match field.default_expr() {
                    Some(expr) => {
                        format!(" DEFAULT {expr}")
                    }
                    None => "".to_string(),
                };
                let computed_expr = match field.computed_expr() {
                    Some(ComputedExpr::Virtual(expr)) => {
                        format!(" AS ({expr}) VIRTUAL")
                    }
                    Some(ComputedExpr::Stored(expr)) => {
                        format!(" AS ({expr}) STORED")
                    }
                    _ => "".to_string(),
                };
                // compatibility: creating table in the old planner will not have `fields_comments`
                let comment =
                    if field_comments.len() == n_fields && !field_comments[&(idx as u32)].is_empty() {
                        // make the display more readable.
                        // can not use debug print, will add double quote
                        format!(" COMMENT '{}'", comment.as_str(),)
                    } else {
                        "".to_string()
                    };
                let column_str = format!(
                    "  {} {}{}{}{}{}",
                    display_ident(field.name(), quoted_ident_case_sensitive, sql_dialect),
                    field.data_type().remove_recursive_nullable().sql_name(),
                    nullable,
                    default_expr,
                    computed_expr,
                    comment
                );

                create_defs.push(column_str);
            }

            let create_defs_str = format!("{}\n", create_defs.join(",\n"));
            dict_create_sql.push_str(&create_defs_str);
        }
        // Append primary keys.
        {
            dict_create_sql.push_str(")\nPRIMARY KEY(");
            let fields = schema.fields.clone();
            for pk_id in pk_id_list {
                let field: &TableField = &fields[pk_id as usize];
                let name = field.name.clone();
                dict_create_sql.push_str(&format!("{},", name));
            }
            dict_create_sql.pop();
            dict_create_sql.push_str(")\n");
        }
        // Append source options.
        {
            dict_create_sql.push_str(&format!(") SOURCE({}\n", source));
            dict_create_sql.push_str("(\n");
            for (key, value) in source_options {
                dict_create_sql.push_str(&format!(" {}='{}'\n", key, value));
            }
            dict_create_sql.push_str("))\n")
        }
        // Append comment.
        {
            dict_create_sql.push_str("COMMENT ");
            dict_create_sql.push_str(&format!("'{}';", comment));
        }
        Ok(dict_create_sql)
    }
}
