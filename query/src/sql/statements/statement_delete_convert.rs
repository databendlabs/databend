// Copyright 2021 Datafuse Labs.
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

use std::convert::TryFrom;

use sqlparser::ast::Expr;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Statement;
use sqlparser::parser::ParserError;

use crate::sql::statements::DfDeleteStatement;

impl TryFrom<Statement> for DfDeleteStatement {
    type Error = ParserError;

    fn try_from(statement: Statement) -> Result<Self, Self::Error> {
        // TODO simplify this
        let (from, selection) = Self::get_body(&statement)?;
        Ok(DfDeleteStatement {
            from: from.clone(),
            selection: selection.clone(),
        })
    }
}

impl DfDeleteStatement {
    fn get_body(query: &Statement) -> Result<(&ObjectName, &Option<Expr>), ParserError> {
        match &query {
            Statement::Delete {
                table_name,
                selection,
            } => Ok((table_name, selection)),
            other => Err(ParserError::ParserError(format!(
                // TODO should be logic err
                "Query {} is not yet implemented",
                other
            ))),
        }
    }
}
