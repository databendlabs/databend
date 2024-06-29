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

use databend_common_ast::ast::Identifier;
use databend_common_exception::ErrorCode;
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
    Unqoted,
}

impl NameResolutionContext {
    pub fn not_found_suggest(&self, ident: &Identifier) -> Option<NameResolutionSuggest> {
        match (
            self.unquoted_ident_case_sensitive,
            self.quoted_ident_case_sensitive,
            ident.is_quoted(),
        ) {
            (false, true, false) => {
                if ident.name.to_lowercase() != ident.name {
                    return Some(NameResolutionSuggest::Quoted);
                }
            }
            (true, false, true) => {
                if ident.name.to_lowercase() != ident.name {
                    return Some(NameResolutionSuggest::Unqoted);
                }
            }
            (_, _, _) => (),
        };
        None
    }

    pub fn table_not_found_suggest_error(&self, table: &Identifier) -> Option<ErrorCode> {
        let name = &table.name;
        self.not_found_suggest(table).map(|suggest| {
            ErrorCode::UnknownTable(match suggest {
                NameResolutionSuggest::Quoted => {
                    format!("Unknown table {table} (unquoted). Did you mean `{name}` (quoted)?",)
                }
                NameResolutionSuggest::Unqoted => {
                    format!("Unknown table {table} (quoted). Did you mean {name} (unquoted)?",)
                }
            })
        })
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
        Identifier::from_name(ident.span, ident.name.to_lowercase())
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
#[visitor(Identifier(enter))]
pub struct IdentifierNormalizer<'a> {
    pub ctx: &'a NameResolutionContext,
}

impl<'a> IdentifierNormalizer<'a> {
    fn enter_identifier(&mut self, ident: &mut Identifier) {
        let normalized_ident = normalize_identifier(ident, self.ctx);
        *ident = normalized_ident;
    }
}
