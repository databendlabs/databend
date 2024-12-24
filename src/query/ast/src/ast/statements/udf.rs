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
pub enum UDFDefinition {
    LambdaUDF {
        parameters: Vec<Identifier>,
        definition: Box<Expr>,
    },
    UDFServer {
        arg_types: Vec<TypeName>,
        return_type: TypeName,
        address: String,
        handler: String,
        language: String,
    },
    UDFScript {
        arg_types: Vec<TypeName>,
        return_type: TypeName,
        code: String,
        handler: String,
        language: String,
        runtime_version: String,
    },
    UDAFServer {
        arg_types: Vec<TypeName>,
        state_fields: Vec<UDAFStateField>,
        return_type: TypeName,
        address: String,
        language: String,
    },
    UDAFScript {
        arg_types: Vec<TypeName>,
        state_fields: Vec<UDAFStateField>,
        return_type: TypeName,
        code: String,
        language: String,
        runtime_version: String,
    },
}

impl Display for UDFDefinition {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            UDFDefinition::LambdaUDF {
                parameters,
                definition,
            } => {
                write!(f, "AS (")?;
                write_comma_separated_list(f, parameters)?;
                write!(f, ") -> {definition}")?;
            }
            UDFDefinition::UDFServer {
                arg_types,
                return_type,
                address,
                handler,
                language,
            } => {
                write!(f, "( ")?;
                write_comma_separated_list(f, arg_types)?;
                write!(
                    f,
                    " ) RETURNS {return_type} LANGUAGE {language} HANDLER = '{handler}' ADDRESS = '{address}'"
                )?;
            }
            UDFDefinition::UDFScript {
                arg_types,
                return_type,
                code,
                handler,
                language,
                runtime_version: _,
            } => {
                write!(f, "( ")?;
                write_comma_separated_list(f, arg_types)?;
                write!(
                    f,
                    " ) RETURNS {return_type} LANGUAGE {language} HANDLER = '{handler}' AS $$\n{code}\n$$"
                )?;
            }
            UDFDefinition::UDAFServer {
                arg_types,
                state_fields: state_types,
                return_type,
                address,
                language,
            } => {
                write!(f, "( ")?;
                write_comma_separated_list(f, arg_types)?;
                write!(f, " ) STATE {{ ")?;
                write_comma_separated_list(f, state_types)?;
                write!(
                    f,
                    " }} RETURNS {return_type} LANGUAGE {language} ADDRESS = '{address}'"
                )?;
            }
            UDFDefinition::UDAFScript {
                arg_types,
                state_fields: state_types,
                return_type,
                code,
                language,
                runtime_version: _,
            } => {
                write!(f, "( ")?;
                write_comma_separated_list(f, arg_types)?;
                write!(f, " ) STATE {{ ")?;
                write_comma_separated_list(f, state_types)?;
                write!(
                    f,
                    " }} RETURNS {return_type} LANGUAGE {language} AS $$\n{code}\n$$"
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct UDAFStateField {
    pub name: Identifier,
    pub type_name: TypeName,
}

impl Display for UDAFStateField {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.type_name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateUDFStmt {
    pub create_option: CreateOption,
    pub udf_name: Identifier,
    pub description: Option<String>,
    pub definition: UDFDefinition,
}

impl Display for CreateUDFStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " FUNCTION")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {} {}", self.udf_name, self.definition)?;
        if let Some(description) = &self.description {
            write!(f, " DESC = '{description}'")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterUDFStmt {
    pub udf_name: Identifier,
    pub description: Option<String>,
    pub definition: UDFDefinition,
}

impl Display for AlterUDFStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER FUNCTION")?;
        write!(f, " {} {}", self.udf_name, self.definition)?;
        if let Some(description) = &self.description {
            write!(f, " DESC = '{description}'")?;
        }
        Ok(())
    }
}
