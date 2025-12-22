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

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IdentifierType;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::quote::ident_needs_quote;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_settings::Settings;
use derive_visitor::VisitorMut;

#[derive(Debug, Clone)]
pub struct NameResolutionContext {
    pub unquoted_ident_case_sensitive: bool,
    pub quoted_ident_case_sensitive: bool,
    pub deny_column_reference: bool,
}

pub enum NameResolutionSuggest {
    Quoted,
    Unquoted,
}

impl NameResolutionContext {
    pub fn not_found_suggest(&self, ident: &Identifier) -> Option<NameResolutionSuggest> {
        if !ident.name.chars().any(|c| c.is_ascii_uppercase()) {
            return None;
        }
        match (
            self.unquoted_ident_case_sensitive,
            self.quoted_ident_case_sensitive,
            ident.is_quoted(),
        ) {
            (false, true, false) => Some(NameResolutionSuggest::Quoted),
            (true, false, true) if !ident_needs_quote(&ident.name) => {
                Some(NameResolutionSuggest::Unquoted)
            }
            _ => None,
        }
    }
}

impl Default for NameResolutionContext {
    fn default() -> Self {
        Self {
            unquoted_ident_case_sensitive: false,
            quoted_ident_case_sensitive: true,
            deny_column_reference: false,
        }
    }
}

impl TryFrom<&Settings> for NameResolutionContext {
    type Error = databend_common_exception::ErrorCode;

    fn try_from(settings: &Settings) -> databend_common_exception::Result<Self> {
        let unquoted_ident_case_sensitive = settings.get_unquoted_ident_case_sensitive()?;
        let quoted_ident_case_sensitive = settings.get_quoted_ident_case_sensitive()?;

        Ok(Self {
            unquoted_ident_case_sensitive,
            quoted_ident_case_sensitive,
            deny_column_reference: false,
        })
    }
}

/// Normalize identifier with given `NameResolutionContext`
pub fn normalize_identifier(ident: &Identifier, context: &NameResolutionContext) -> Identifier {
    if (ident.is_quoted() && context.quoted_ident_case_sensitive)
        || (!ident.is_quoted() && context.unquoted_ident_case_sensitive)
    {
        ident.clone()
    } else {
        // Preserve the quote information when creating a new identifier
        Identifier {
            span: ident.span,
            name: ident.name.to_lowercase(),
            quote: ident.quote,
            ident_type: ident.ident_type,
        }
    }
}

pub fn compare_table_name(
    table_name1: &str,
    table_name2: &str,
    context: &NameResolutionContext,
) -> bool {
    if context.unquoted_ident_case_sensitive || !context.quoted_ident_case_sensitive {
        table_name1 == table_name2
    } else {
        table_name1.to_lowercase() == table_name2.to_lowercase()
    }
}

#[derive(VisitorMut)]
#[visitor(Identifier(enter), MapAccessor)]
pub struct IdentifierNormalizer<'a> {
    ctx: &'a NameResolutionContext,
    in_map_accessor: bool,
}

impl<'a> IdentifierNormalizer<'a> {
    pub fn new(ctx: &'a NameResolutionContext) -> Self {
        Self {
            ctx,
            in_map_accessor: false,
        }
    }

    fn enter_identifier(&mut self, ident: &mut Identifier) {
        // Skip normalization if inside a MapAccessor,
        // because MapAccessor is used to extract internal fields of nested types,
        // altering the case may prevent the desired data from being retrieved.
        if !self.in_map_accessor {
            let normalized_ident = normalize_identifier(ident, self.ctx);
            *ident = normalized_ident;
        }
    }

    fn enter_map_accessor(&mut self, _accessor: &mut MapAccessor) {
        // Set flag to true before processing the identifier inside the accessor
        self.in_map_accessor = true;
    }

    fn exit_map_accessor(&mut self, _accessor: &mut MapAccessor) {
        // Reset the flag after processing
        self.in_map_accessor = false;
    }
}

#[derive(VisitorMut)]
#[visitor(Identifier(enter))]
pub struct VariableNormalizer<'a> {
    pub ctx: &'a NameResolutionContext,
    pub table_ctx: Arc<dyn TableContext>,
    error: Option<ErrorCode>,
}

impl<'a> VariableNormalizer<'a> {
    pub fn new(ctx: &'a NameResolutionContext, table_ctx: Arc<dyn TableContext>) -> Self {
        Self {
            ctx,
            table_ctx,
            error: None,
        }
    }

    pub fn render_error(&self) -> Result<()> {
        match &self.error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    fn enter_identifier(&mut self, ident: &mut Identifier) {
        if ident.is_variable() {
            let mut normalized_ident = normalize_identifier(ident, self.ctx);

            let scalar = self.table_ctx.get_variable(&normalized_ident.name);
            if let Some(Scalar::String(s)) = scalar {
                normalized_ident.name = s;
                normalized_ident.ident_type = IdentifierType::None;
            } else {
                self.error = Some(ErrorCode::SemanticError(format!(
                    "invalid variable identifier {} in session",
                    normalized_ident.name
                )));
            }
            *ident = normalized_ident;
        } else if ident.is_hole() {
            self.error = Some(ErrorCode::SemanticError(format!(
                "invalid hole identifier {}, maybe you want to use ${}",
                ident.name, ident.name,
            )));
        }
    }
}
