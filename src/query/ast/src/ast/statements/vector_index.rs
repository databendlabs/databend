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

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::Expr;
use crate::ast::Identifier;
use crate::parser::token::TokenKind;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateVectorIndexStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub index_type: TokenKind,
    pub column: Identifier,
    pub metric_type: TokenKind,
    pub paras: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VectorIndexType {
    IVFFLAT,
}

impl Display for CreateVectorIndexStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let index_type = match self.index_type {
            TokenKind::IVFFLAT => "IVFFLAT",
            _ => {
                unreachable!()
            }
        };
        let metric_type = match self.metric_type {
            TokenKind::COSINE => "cosine distance",
            _ => {
                unreachable!()
            }
        };
        write!(
            f,
            "CREATE INDEX {} ON {} USING {}({})",
            index_type, self.table, metric_type, self.column
        )
    }
}
