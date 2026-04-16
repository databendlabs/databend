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
use databend_common_ast::ast::quote::ident_opt_quote;
use databend_common_ast::parser::Dialect;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::VisitorMut;
use databend_common_ast::visit::WalkMut;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_settings::Settings;

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
    // rely on normalize_identifier() do not change quote
    pub fn is_case_sensitive(&self, ident: &Identifier) -> bool {
        (ident.is_quoted() && self.quoted_ident_case_sensitive)
            || (!ident.is_quoted() && self.unquoted_ident_case_sensitive)
    }

    pub fn not_found_suggest(&self, ident: &Identifier) -> Option<NameResolutionSuggest> {
        if !ident.name.chars().any(|c| c.is_uppercase()) {
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

#[derive(Debug, Clone)]
pub struct IdentifierNormalizer<'a> {
    ctx: &'a NameResolutionContext,
    in_map_accessor: bool,
}

impl<'a> VisitorMut for IdentifierNormalizer<'a> {
    fn visit_identifier(&mut self, ident: &mut Identifier) -> std::result::Result<VisitControl, !> {
        // Skip normalization if inside a MapAccessor,
        // because MapAccessor is used to extract internal fields of nested types,
        // altering the case may prevent the desired data from being retrieved.
        if !self.in_map_accessor {
            let normalized_ident = normalize_identifier(ident, self.ctx);
            *ident = normalized_ident;
        }
        Ok(VisitControl::Continue)
    }

    fn visit_map_accessor(
        &mut self,
        accessor: &mut MapAccessor,
    ) -> std::result::Result<VisitControl, !> {
        let prev = self.in_map_accessor;
        self.in_map_accessor = true;

        let result = match accessor {
            MapAccessor::Bracket { key } => key.walk_mut(self),
            MapAccessor::Colon { key } => key.walk_mut(self),
            MapAccessor::DotNumber { .. } => Ok(VisitControl::Continue),
        };

        self.in_map_accessor = prev;
        match result {
            Ok(VisitControl::Break(v)) => Ok(VisitControl::Break(v)),
            Ok(VisitControl::Continue) | Ok(VisitControl::SkipChildren) => {
                Ok(VisitControl::SkipChildren)
            }
        }
    }
}

impl<'a> IdentifierNormalizer<'a> {
    pub fn new(ctx: &'a NameResolutionContext) -> Self {
        Self {
            ctx,
            in_map_accessor: false,
        }
    }
}

#[derive(Clone)]
pub struct VariableNormalizer<'a> {
    pub ctx: &'a NameResolutionContext,
    pub table_ctx: Arc<dyn TableContext>,
    error: Option<ErrorCode>,
}

impl<'a> VisitorMut for VariableNormalizer<'a> {
    fn visit_identifier(&mut self, ident: &mut Identifier) -> std::result::Result<VisitControl, !> {
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
        Ok(VisitControl::Continue)
    }
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
}

#[derive(Debug, Clone)]
pub struct ClusterKeyNormalizer {
    pub force_quoted_ident: bool,
    pub unquoted_ident_case_sensitive: bool,
    pub quoted_ident_case_sensitive: bool,
    pub sql_dialect: Dialect,
}

impl VisitorMut for ClusterKeyNormalizer {
    fn visit_identifier(&mut self, ident: &mut Identifier) -> std::result::Result<VisitControl, !> {
        let case_sensitive = (ident.is_quoted() && self.quoted_ident_case_sensitive)
            || (!ident.is_quoted() && self.unquoted_ident_case_sensitive);
        if !case_sensitive {
            ident.name = ident.name.to_lowercase();
        }
        ident.quote = ident_opt_quote(
            &ident.name,
            self.force_quoted_ident,
            self.quoted_ident_case_sensitive,
            self.sql_dialect,
        );
        Ok(VisitControl::Continue)
    }
}

impl ClusterKeyNormalizer {}
