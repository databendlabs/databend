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
use databend_common_meta_kvapi::kvapi::Key;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UdfName {
    pub tenant: String,
    pub name: String,
}

impl UdfName {
    pub fn new(tenant: impl ToString, name: impl ToString) -> Self {
        Self {
            tenant: tenant.to_string(),
            name: name.to_string(),
        }
    }

    pub fn tenant_prefix(&self) -> String {
        Self::new(&self.tenant, "").to_string_key()
    }
}

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
pub struct UDFInterpreter {
    pub code: String,
    pub handler: String,
    pub language: String,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub runtime_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UDFDefinition {
    LambdaUDF(LambdaUDF),
    UDFServer(UDFServer),
    UDFInterpreter(UDFInterpreter),
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

    pub fn create_udf_interpreter(
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
            definition: UDFDefinition::UDFInterpreter(UDFInterpreter {
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

            UDFDefinition::UDFInterpreter(UDFInterpreter {
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
        }
        Ok(())
    }
}

mod kv_api_impl {
    use databend_common_meta_kvapi::kvapi;

    use super::UdfName;
    use crate::principal::UserDefinedFunction;
    use crate::tenant::Tenant;

    impl kvapi::Key for UdfName {
        const PREFIX: &'static str = "__fd_udfs";

        type ValueType = UserDefinedFunction;

        /// It belongs to a tenant
        fn parent(&self) -> Option<String> {
            Some(Tenant::new(&self.tenant).to_string_key())
        }

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(&self.tenant)
                .push_str(&self.name)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let tenant = p.next_str()?;
            let name = p.next_str()?;
            p.done()?;

            Ok(UdfName { tenant, name })
        }
    }

    impl kvapi::Value for UserDefinedFunction {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
