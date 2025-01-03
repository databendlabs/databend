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

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LambdaUDF {
    pub parameters: Vec<String>,
    pub definition: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UDFServer {
    pub address: String,
    pub handler: String,
    pub language: String,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UDFScript {
    pub code: String,
    pub handler: String,
    pub language: String,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub runtime_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UDAFScript {
    pub code: String,
    pub language: String,
    // aggregate function input types
    pub arg_types: Vec<DataType>,
    // aggregate function state fields
    pub state_fields: Vec<DataField>,
    pub return_type: DataType,
    pub runtime_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UDFDefinition {
    LambdaUDF(LambdaUDF),
    UDFServer(UDFServer),
    UDFScript(UDFScript),
    UDAFScript(UDAFScript),
}

impl UDFDefinition {
    pub fn category(&self) -> &str {
        match self {
            Self::LambdaUDF(_) => "LambdaUDF",
            Self::UDFServer(_) => "UDFServer",
            Self::UDFScript(_) => "UDFScript",
            Self::UDAFScript(_) => "UDAFScript",
        }
    }

    pub fn is_aggregate(&self) -> bool {
        match self {
            Self::LambdaUDF(_) => false,
            Self::UDFServer(_) => false,
            Self::UDFScript(_) => false,
            Self::UDAFScript(_) => true,
        }
    }

    pub fn language(&self) -> &str {
        match self {
            Self::LambdaUDF(_) => "SQL",
            Self::UDFServer(x) => x.language.as_str(),
            Self::UDFScript(x) => x.language.as_str(),
            Self::UDAFScript(x) => x.language.as_str(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserDefinedFunction {
    pub name: String,
    pub description: String,
    pub definition: UDFDefinition,
    pub created_on: DateTime<Utc>,
}

impl UserDefinedFunction {
    pub fn create_lambda_udf(
        name: &str,
        parameters: Vec<String>,
        definition: &str,
        description: &str,
    ) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            definition: UDFDefinition::LambdaUDF(LambdaUDF {
                parameters,
                definition: definition.to_string(),
            }),
            created_on: Utc::now(),
        }
    }

    pub fn create_udf_server(
        name: &str,
        address: &str,
        handler: &str,
        language: &str,
        arg_types: Vec<DataType>,
        return_type: DataType,
        description: &str,
    ) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            definition: UDFDefinition::UDFServer(UDFServer {
                address: address.to_string(),
                handler: handler.to_string(),
                language: language.to_string(),
                arg_types,
                return_type,
            }),
            created_on: Utc::now(),
        }
    }

    pub fn create_udf_script(
        name: &str,
        code: &str,
        handler: &str,
        language: &str,
        arg_types: Vec<DataType>,
        return_type: DataType,
        runtime_version: &str,
        description: &str,
    ) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            definition: UDFDefinition::UDFScript(UDFScript {
                code: code.to_string(),
                handler: handler.to_string(),
                language: language.to_string(),
                arg_types,
                return_type,
                runtime_version: runtime_version.to_string(),
            }),
            created_on: Utc::now(),
        }
    }
}

impl Display for UDFDefinition {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, " (")?;
        match self {
            UDFDefinition::LambdaUDF(LambdaUDF {
                parameters,
                definition,
            }) => {
                for (i, item) in parameters.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, ") -> {definition}")?;
            }
            UDFDefinition::UDFServer(UDFServer {
                address,
                arg_types,
                return_type,
                handler,
                language,
            }) => {
                for (i, item) in arg_types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(
                    f,
                    ") RETURNS {return_type} LANGUAGE {language} HANDLER = {handler} ADDRESS = {address}"
                )?;
            }
            UDFDefinition::UDFScript(UDFScript {
                code,
                arg_types,
                return_type,
                handler,
                language,
                runtime_version,
            }) => {
                for (i, item) in arg_types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(
                    f,
                    ") RETURNS {return_type} LANGUAGE {language} RUNTIME_VERSION = {runtime_version} HANDLER = {handler} AS $${code}$$"
                )?;
            }
            UDFDefinition::UDAFScript(UDAFScript {
                code,
                arg_types,
                state_fields,
                return_type,
                language,
                runtime_version,
            }) => {
                for (i, item) in arg_types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, ") STATE {{ ")?;
                for (i, item) in state_fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", item.name(), item.data_type())?;
                }
                write!(f, " }} RETURNS {return_type} LANGUAGE {language} RUNTIME_VERSION = {runtime_version} AS $${code}$$")?;
            }
        }
        Ok(())
    }
}
