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

use databend_common_ast::ast::quote::ident_needs_quote;
use databend_common_ast::ast::Identifier;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use derive_visitor::VisitorMut;

#[derive(Clone)]
pub struct NameResolutionContext {
    pub unquoted_ident_case_sensitive: bool,
    pub quoted_ident_case_sensitive: bool,
    pub ctx: Option<Arc<dyn TableContext>>,
}

pub enum NameResolutionSuggest {
    Quoted,
    Unqoted,
}

impl NameResolutionContext {
    pub fn try_from_context(ctx: Arc<dyn TableContext>) -> Result<Self> {
        let settings = ctx.get_settings();
        let s = Self {
            unquoted_ident_case_sensitive: settings.get_unquoted_ident_case_sensitive()?,
            quoted_ident_case_sensitive: settings.get_quoted_ident_case_sensitive()?,
            ctx: Some(ctx),
        };
        Ok(s)
    }

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
                Some(NameResolutionSuggest::Unqoted)
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
            ctx: None,
        }
    }
}

/// Normalize identifier with given `NameResolutionContext`
pub fn normalize_identifier(ident: &mut Identifier, context: &NameResolutionContext) {
    let no_need_normalize = (ident.is_quoted() && context.quoted_ident_case_sensitive)
        || (!ident.is_quoted() && context.unquoted_ident_case_sensitive)
        || !ident.name.chars().any(|c| c.is_ascii_uppercase());

    if !no_need_normalize {
        ident.normalized_name = Some(Box::new(ident.name.to_lowercase()));
    }
}

#[derive(VisitorMut)]
#[visitor(Identifier(enter))]
pub struct IdentifierNormalizer<'a> {
    pub ctx: &'a NameResolutionContext,
    pub error: Option<ErrorCode>,
}

impl<'a> IdentifierNormalizer<'a> {
    pub fn new(ctx: &'a NameResolutionContext) -> Self {
        Self { ctx, error: None }
    }

    pub fn render_error(&self) -> Result<()> {
        match &self.error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}

impl<'a> IdentifierNormalizer<'a> {
    fn enter_identifier(&mut self, ident: &mut Identifier) {
        if ident.is_hole {
            self.error = Some(ErrorCode::SemanticError(format!(
                "invalid hole identifier {}, maybe you want to use ${}",
                ident.name, ident.name,
            )));
        }

        normalize_identifier(ident, self.ctx);
        if ident.is_variable {
            let scalar = self
                .ctx
                .ctx
                .as_ref()
                .and_then(|c| c.get_variable(&ident.normalized_name()));

            if let Some(Scalar::String(s)) = scalar {
                ident.normalized_name = Some(Box::new(s));
                ident.is_variable = false;
            } else {
                self.error = Some(ErrorCode::SemanticError(format!(
                    "invalid variable identifier {} in session",
                    ident.name
                )));
            }
        }
    }
}
