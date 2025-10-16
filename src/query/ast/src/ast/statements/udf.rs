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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
use itertools::Itertools;

use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_list;
use crate::ast::CreateOption;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::TypeName;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum UDFArgs {
    Types(Vec<TypeName>),
    NameWithTypes(Vec<(Identifier, TypeName)>),
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum UDFDefinition {
    LambdaUDF {
        parameters: Vec<Identifier>,
        definition: Box<Expr>,
    },
    UDFServer {
        arg_types: UDFArgs,
        return_type: TypeName,
        address: String,
        handler: String,
        headers: BTreeMap<String, String>,
        language: String,
        immutable: Option<bool>,
    },
    UDFScript {
        arg_types: UDFArgs,
        return_type: TypeName,
        code: String,
        imports: Vec<String>,
        packages: Vec<String>,
        handler: String,
        language: String,
        runtime_version: String,
        immutable: Option<bool>,
    },
    UDAFServer {
        arg_types: UDFArgs,
        state_fields: Vec<UDAFStateField>,
        return_type: TypeName,
        address: String,
        headers: BTreeMap<String, String>,
        language: String,
    },
    UDAFScript {
        arg_types: UDFArgs,
        state_fields: Vec<UDAFStateField>,
        return_type: TypeName,
        imports: Vec<String>,
        packages: Vec<String>,
        code: String,
        language: String,
        runtime_version: String,
    },
    UDTFSql {
        arg_types: Vec<(Identifier, TypeName)>,
        return_types: Vec<(Identifier, TypeName)>,
        sql: String,
    },
    ScalarUDF {
        arg_types: Vec<(Identifier, TypeName)>,
        definition: String,
        return_type: TypeName,
    },
}

impl UDFArgs {
    pub fn len(&self) -> usize {
        match self {
            UDFArgs::Types(types) => types.len(),
            UDFArgs::NameWithTypes(name_with_types) => name_with_types.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn types_iter(&self) -> Box<dyn Iterator<Item = &TypeName> + '_> {
        match self {
            UDFArgs::Types(types) => Box::new(types.iter()),
            UDFArgs::NameWithTypes(name_with_types) => {
                Box::new(name_with_types.iter().map(|(_, ty)| ty))
            }
        }
    }
}

impl Display for UDFArgs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UDFArgs::Types(types) => {
                write_comma_separated_list(f, types)?;
            }
            UDFArgs::NameWithTypes(name_with_types) => {
                write_comma_separated_list(
                    f,
                    name_with_types
                        .iter()
                        .map(|(name, ty)| format!("{name} {ty}")),
                )?;
            }
        }
        Ok(())
    }
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
                headers,
                language,
                immutable,
            } => {
                write!(f, "( {arg_types}")?;
                write!(f, " ) RETURNS {return_type} LANGUAGE {language}")?;
                if let Some(immutable) = immutable {
                    if *immutable {
                        write!(f, " IMMUTABLE")?;
                    } else {
                        write!(f, " VOLATILE")?;
                    }
                }
                write!(f, " HANDLER = '{handler}'")?;
                if !headers.is_empty() {
                    write!(f, " HEADERS = (")?;
                    for (i, (key, value)) in headers.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "'{key}' = '{value}'")?;
                    }
                    write!(f, ")")?;
                }
                write!(f, " ADDRESS = '{address}'")?;
            }
            UDFDefinition::UDFScript {
                arg_types,
                return_type,
                code,
                handler,
                language,
                runtime_version: _,
                imports,
                packages,
                immutable,
            } => {
                write!(f, "( {arg_types}")?;
                let imports = imports
                    .iter()
                    .map(|s| QuotedString(s, '\'').to_string())
                    .join(",");
                let packages = packages
                    .iter()
                    .map(|s| QuotedString(s, '\'').to_string())
                    .join(",");
                write!(f, " ) RETURNS {return_type} LANGUAGE {language}")?;
                if let Some(immutable) = immutable {
                    if *immutable {
                        write!(f, " IMMUTABLE")?;
                    } else {
                        write!(f, " VOLATILE")?;
                    }
                }
                write!(
                    f,
                    " IMPORTS = ({}) PACKAGES = ({}) HANDLER = '{handler}' AS $$\n{code}\n$$",
                    imports, packages
                )?;
            }
            UDFDefinition::UDAFServer {
                arg_types,
                state_fields: state_types,
                return_type,
                address,
                headers,
                language,
            } => {
                write!(f, "( {arg_types}")?;
                write!(f, " ) STATE {{ ")?;
                write_comma_separated_list(f, state_types)?;
                write!(f, " }} RETURNS {return_type} LANGUAGE {language}")?;
                if !headers.is_empty() {
                    write!(f, " HEADERS = (")?;
                    for (i, (key, value)) in headers.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "'{key}' = '{value}'")?;
                    }
                    write!(f, ")")?;
                }
                write!(f, " ADDRESS = '{address}'")?;
            }
            UDFDefinition::UDTFSql {
                arg_types,
                return_types,
                sql,
            } => {
                write!(f, "(")?;
                write_comma_separated_list(
                    f,
                    arg_types.iter().map(|(name, ty)| format!("{name} {ty}")),
                )?;
                write!(f, ") RETURNS TABLE (")?;
                write_comma_separated_list(
                    f,
                    return_types.iter().map(|(name, ty)| format!("{name} {ty}")),
                )?;
                write!(f, ") AS $$\n{sql}\n$$")?;
            }
            UDFDefinition::ScalarUDF {
                arg_types,
                definition,
                return_type,
            } => {
                write!(f, "(")?;
                write_comma_separated_list(
                    f,
                    arg_types.iter().map(|(name, ty)| format!("{name} {ty}")),
                )?;
                write!(f, ") RETURNS {return_type} AS $$\n{definition}\n$$")?;
            }
            UDFDefinition::UDAFScript {
                arg_types,
                state_fields: state_types,
                return_type,
                code,
                language,
                runtime_version: _,
                imports,
                packages,
            } => {
                let imports = imports
                    .iter()
                    .map(|s| QuotedString(s, '\'').to_string())
                    .join(",");
                let packages = packages
                    .iter()
                    .map(|s| QuotedString(s, '\'').to_string())
                    .join(",");

                write!(f, "( {arg_types}")?;
                write!(f, " ) STATE {{ ")?;
                write_comma_separated_list(f, state_types)?;
                write!(
                    f,
                    " }} RETURNS {return_type} LANGUAGE {language} IMPORTS = ({}) PACKAGES = ({}) AS $$\n{code}\n$$",
                    imports, packages
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
