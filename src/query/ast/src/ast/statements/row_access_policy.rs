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
use crate::ast::Identifier;
use crate::ast::TypeName;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RowAccessPolicyType {
    pub name: String,
    pub data_type: TypeName,
}

impl Display for RowAccessPolicyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RowAccessPolicyDefinition {
    pub parameters: Vec<RowAccessPolicyType>,
    pub definition: Box<Expr>,
}

impl Display for RowAccessPolicyDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        write_comma_separated_list(f, &self.parameters)?;
        write!(f, ")")?;
        write!(f, " RETURNS BOOLEAN ->")?;
        write!(f, " {}", self.definition)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateRowAccessPolicyStmt {
    pub create_option: CreateOption,
    pub name: Identifier,
    pub description: Option<String>,
    pub definition: RowAccessPolicyDefinition,
}

// CREATE [ OR REPLACE ] ROW ACCESS POLICY [ IF NOT EXISTS ] <name> AS
// ( <arg_name> <arg_type> [ , ... ] ) RETURNS BOOLEAN -> <body>
// [ COMMENT = '<string_literal>' ]
impl Display for CreateRowAccessPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " ROW ACCESS POLICY")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {} AS {}", self.name, self.definition)?;
        if let Some(description) = &self.description {
            write!(f, " COMMENT = '{description}'")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropRowAccessPolicyStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropRowAccessPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP ROW ACCESS POLICY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescRowAccessPolicyStmt {
    pub name: String,
}

impl Display for DescRowAccessPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE ROW ACCESS POLICY {}", self.name)?;

        Ok(())
    }
}
