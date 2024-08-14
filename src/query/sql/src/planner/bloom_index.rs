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

use std::collections::BTreeMap;
use std::str::FromStr;

use databend_common_ast::parser::parse_comma_separated_idents;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::is_stream_column_id;
use databend_common_expression::ComputedExpr;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;

use crate::normalize_identifier;
use crate::planner::semantic::NameResolutionContext;

#[derive(Clone)]
pub enum BloomIndexColumns {
    /// Default, all columns that support bloom index.
    All,
    /// Specify with column names.
    Specify(Vec<String>),
    /// The column of bloom index is empty.
    None,
}

impl FromStr for BloomIndexColumns {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Ok(BloomIndexColumns::None);
        }

        let sql_dialect = Dialect::default();
        let tokens = tokenize_sql(s)?;
        let idents = parse_comma_separated_idents(&tokens, sql_dialect)?;

        let settings = Settings::create(Tenant::new_literal("dummy"));
        let name_resolution_ctx =
            NameResolutionContext::try_new(settings.as_ref(), Default::default())?;

        let mut cols = Vec::with_capacity(idents.len());
        idents
            .into_iter()
            .for_each(|ident| cols.push(normalize_identifier(&ident, &name_resolution_ctx).name));

        Ok(BloomIndexColumns::Specify(cols))
    }
}

impl BloomIndexColumns {
    /// Verify the definition based on schema.
    pub fn verify_definition<F>(
        definition: &str,
        schema: TableSchemaRef,
        verify_type: F,
    ) -> Result<()>
    where
        F: Fn(&TableDataType) -> bool,
    {
        if definition.trim().is_empty() {
            return Ok(());
        }

        let settings = Settings::create(Tenant::new_literal("dummy"));
        let name_resolution_ctx =
            NameResolutionContext::try_new(settings.as_ref(), Default::default())?;

        let sql_dialect = Dialect::default();
        let tokens = tokenize_sql(definition)?;
        let idents = parse_comma_separated_idents(&tokens, sql_dialect)?;
        for ident in idents.iter() {
            let name = &normalize_identifier(ident, &name_resolution_ctx).name;
            let field = schema.field_with_name(name)?;

            if matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_))) {
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "The value specified for computed column '{}' is not allowed for bloom index",
                    name
                )));
            }

            let data_type = field.data_type();
            if !verify_type(data_type) {
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "Unsupported data type '{}' for bloom index",
                    data_type
                )));
            }
        }
        Ok(())
    }

    /// Get table field based on the BloomIndexColumns and schema.
    pub fn bloom_index_fields<F>(
        &self,
        schema: TableSchemaRef,
        verify_type: F,
    ) -> Result<BTreeMap<FieldIndex, TableField>>
    where
        F: Fn(&TableDataType) -> bool,
    {
        let source_schema = schema.remove_virtual_computed_fields();
        let mut fields_map = BTreeMap::new();
        match self {
            BloomIndexColumns::All => {
                for (i, field) in source_schema.fields.into_iter().enumerate() {
                    // Ignore stream column.
                    if is_stream_column_id(field.column_id) {
                        continue;
                    }

                    if verify_type(field.data_type()) {
                        fields_map.insert(i, field);
                    }
                }
            }
            BloomIndexColumns::Specify(cols) => {
                for col in cols {
                    let field_index = source_schema.index_of(col)?;
                    let field = source_schema.fields[field_index].clone();
                    let data_type = field.data_type();
                    if !verify_type(data_type) {
                        return Err(ErrorCode::BadArguments(format!(
                            "Unsupported data type for bloom index: {:?}",
                            data_type
                        )));
                    }
                    fields_map.insert(field_index, field);
                }
            }
            BloomIndexColumns::None => (),
        }
        Ok(fields_map)
    }
}
