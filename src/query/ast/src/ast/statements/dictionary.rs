use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::write_space_separated_string_map;
use crate::ast::ColumnDefinition;
use crate::ast::CreateOption;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateDictionaryStmt {
    pub create_option: CreateOption,
    pub dictionary_name: Identifier,
    pub columns: Vec<ColumnDefinition>,
    pub primary_keys: Vec<Identifier>,
    pub source_name: Identifier,
    pub source_options: BTreeMap<String, String>,
    pub comment: Option<String>,
}

impl Display for CreateDictionaryStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "DICTIONARY ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} ", &self.dictionary_name)?;
        if !self.columns.is_empty() {
            write!(f, "(")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        if !self.primary_keys.is_empty() {
            write!(f, "PRIMARY KEY(")?;
            write_comma_separated_list(f, &self.primary_keys)?;
            write!(f, ")")?;
        }
        write!(f, "SOURCE(")?;
        write!(f, "{}( ", &self.source_name)?;
        if !self.source_options.is_empty() {
            write_space_separated_string_map(f, &self.source_options)?;
        }
        write!(f, ")")?;
        if let Some(comment) = &self.comment {
            write!(f,"COMMENT {comment}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropDictionaryStmt {
    pub if_exists: bool,
    pub dictionary_name: Identifier,
}

impl Display for DropDictionaryStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP DICTIONARY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", &self.dictionary_name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowCreateDictionaryStmt {
    pub dictionary_name: Identifier,
}

impl Display for ShowCreateDictionaryStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW CREATE DICTIONARY ")?;
        write!(f, "{}", &self.dictionary_name)?;
        Ok(())
    }
}
