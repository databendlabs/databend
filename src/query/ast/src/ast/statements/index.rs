use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::Expr;
use crate::ast::Identifier;
use crate::parser::token::TokenKind;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub index_type: TokenKind,
    pub column: Identifier,
    pub metric_type: TokenKind,
    pub paras: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    IVFFLAT,
}

impl Display for CreateIndexStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let index_type = match self.index_type {
            TokenKind::IVFFLAT => "IVFFLAT",
            _ => {
                unreachable!()
            }
        };
        let metric_type = match self.metric_type {
            TokenKind::IP => "inner product",
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
