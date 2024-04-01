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

use databend_common_ast::parser::quote::quote_ident;
use databend_common_ast::parser::Dialect;

pub fn format_name(name: &str, quoted_ident_case_sensitive: bool, dialect: Dialect) -> String {
    // Db-s -> "Db-s" ; dbs -> dbs
    if name.chars().find(|c| c.is_ascii_uppercase()).is_some() && quoted_ident_case_sensitive {
        quote_ident(name, dialect.default_ident_quote(), true)
    } else {
        quote_ident(name, dialect.default_ident_quote(), false)
    }
}
