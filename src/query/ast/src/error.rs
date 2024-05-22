use std::fmt::Display;
use std::fmt::Formatter;

use crate::span::pretty_print_error;
use crate::Span;

pub type Result<T> = std::result::Result<T, ParseError>;

#[derive(Debug)]
pub struct ParseError(pub Span, pub String);

impl ParseError {
    /// Pretty display the error message onto source if span is available.
    pub fn display_with_source(mut self, source: &str) -> Self {
        if let Some(span) = self.0.take() {
            self.1 = pretty_print_error(source, vec![(span, self.1.to_string())]);
        }
        self
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.1)
    }
}
