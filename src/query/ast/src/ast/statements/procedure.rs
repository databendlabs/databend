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

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::CreateOption;
use crate::ast::Expr;
use crate::ast::TypeName;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ExecuteImmediateStmt {
    pub script: String,
}

impl Display for ExecuteImmediateStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "EXECUTE IMMEDIATE $$\n{}\n$$", self.script)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ProcedureType {
    pub name: Option<String>,
    pub data_type: TypeName,
}

impl Display for ProcedureType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{} {}", name, self.data_type)
        } else {
            write!(f, "{}", self.data_type)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum ProcedureLanguage {
    SQL,
}

impl Display for ProcedureLanguage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcedureLanguage::SQL => write!(f, "LANGUAGE SQL "),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ProcedureIdentity {
    pub name: String,
    pub args_type: String,
}

impl Display for ProcedureIdentity {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}({})", &self.name, &self.args_type,)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateProcedureStmt {
    pub create_option: CreateOption,
    pub name: ProcedureIdentity,
    pub language: ProcedureLanguage,
    // TODO(eason): Now args is alwarys none, but maybe we also need to consider arg name?
    pub args: Option<Vec<ProcedureType>>,
    pub return_type: Vec<ProcedureType>,
    pub comment: Option<String>,
    pub script: String,
}

impl Display for CreateProcedureStmt {
    // CREATE [ OR REPLACE ] PROCEDURE <name> ()
    // RETURNS { <result_data_type> }[ NOT NULL ]
    // LANGUAGE SQL
    // [ COMMENT = '<string_literal>' ] AS <procedure_definition>
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " PROCEDURE")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.name.name)?;
        if let Some(args) = &self.args {
            if args.is_empty() {
                write!(f, "()")?;
            } else {
                write!(f, "(")?;
                write_comma_separated_list(f, args.clone())?;
                write!(f, ")")?;
            }
        } else {
            write!(f, "()")?;
        }
        if self.return_type.len() == 1 {
            if let Some(name) = &self.return_type[0].name {
                write!(
                    f,
                    " RETURNS TABLE({} {})",
                    name, self.return_type[0].data_type
                )?;
            } else {
                write!(f, " RETURNS {}", self.return_type[0].data_type)?;
            }
        } else {
            write!(f, " RETURNS TABLE(")?;
            write_comma_separated_list(f, self.return_type.clone())?;
            write!(f, ")")?;
        }

        write!(f, " {}", self.language)?;
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT='{}'", comment)?;
        }
        write!(f, " AS $$\n{}\n$$", self.script)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropProcedureStmt {
    pub if_exists: bool,
    pub name: ProcedureIdentity,
}

impl Display for DropProcedureStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP PROCEDURE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;

        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DescProcedureStmt {
    pub name: ProcedureIdentity,
}

impl Display for DescProcedureStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DESCRIBE PROCEDURE {}", self.name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CallProcedureStmt {
    pub name: String,
    pub args: Vec<Expr>,
}

impl Display for CallProcedureStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let CallProcedureStmt { name, args } = self;
        write!(f, "CALL PROCEDURE {}(", name)?;
        write_comma_separated_list(f, args)?;
        write!(f, ")")?;
        Ok(())
    }
}
