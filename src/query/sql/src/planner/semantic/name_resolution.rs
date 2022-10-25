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

use common_ast::ast::Identifier;
use common_ast::VisitorMut;
use common_settings::Settings;

#[derive(Debug, Clone)]
pub struct NameResolutionContext {
    pub unquoted_ident_case_sensitive: bool,
    pub quoted_ident_case_sensitive: bool,
}

impl Default for NameResolutionContext {
    fn default() -> Self {
        Self {
            unquoted_ident_case_sensitive: false,
            quoted_ident_case_sensitive: true,
        }
    }
}

impl TryFrom<&Settings> for NameResolutionContext {
    type Error = common_exception::ErrorCode;

    fn try_from(settings: &Settings) -> common_exception::Result<Self> {
        let unquoted_ident_case_sensitive = settings.get_unquoted_ident_case_sensitive()?;
        let quoted_ident_case_sensitive = settings.get_quoted_ident_case_sensitive()?;

        Ok(Self {
            unquoted_ident_case_sensitive,
            quoted_ident_case_sensitive,
        })
    }
}

/// Normalize identifier with given `NameResolutionContext`
pub fn normalize_identifier<'a>(
    ident: &Identifier<'a>,
    context: &NameResolutionContext,
) -> Identifier<'a> {
    if (ident.is_quoted() && context.quoted_ident_case_sensitive)
        || (!ident.is_quoted() && context.unquoted_ident_case_sensitive)
    {
        ident.clone()
    } else {
        Identifier {
            name: ident.name.to_lowercase(),
            quote: ident.quote,
            span: ident.span.clone(),
        }
    }
}

pub struct IdentifierNormalizer<'a> {
    pub ctx: &'a NameResolutionContext,
}

impl<'a> VisitorMut for IdentifierNormalizer<'a> {
    fn visit_identifier(&mut self, ident: &mut Identifier<'_>) {
        let normalized_ident = normalize_identifier(ident, self.ctx);
        *ident = normalized_ident;
    }
}
