// Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;
use std::sync::Mutex;

use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;

use super::UDFDefinition;
use super::UDFParser;
use common_functions::is_builtin_function;

#[derive(Default)]
pub struct UDFFactory {
    definitions: HashMap<String, UDFDefinition>,
}

static UDF_FACTORY: Lazy<Mutex<UDFFactory>> = Lazy::new(|| Mutex::new(UDFFactory::default()));

impl UDFFactory {
    pub fn register(
        tenant: &str,
        name: &str,
        parameters: &[String],
        definition: &str,
    ) -> Result<()> {
        if is_builtin_function(name) {
            return Err(ErrorCode::RegisterUDFError(format!(
                "Can not register builtin functions: {} - {}",
                name, definition
            )));
        }

        match UDF_FACTORY.lock() {
            Ok(mut factory) => {
                let mut udf_parser = UDFParser::default();
                let expr = udf_parser.parse_definition(tenant, name, parameters, definition)?;

                let definitions = &mut factory.definitions;
                definitions.insert(
                    UDFFactory::get_udf_key(tenant, name),
                    UDFDefinition::new(parameters.to_owned(), expr),
                );

                Ok(())
            }
            Err(lock_error) => Err(ErrorCode::RegisterUDFError(format!(
                "Can not register UDF: {} - {}, error: {:?}",
                name, definition, lock_error
            ))),
        }
    }

    pub fn unregister(tenant: &str, name: &str) -> Result<()> {
        match UDF_FACTORY.lock() {
            Ok(mut factory) => {
                let definitions = &mut factory.definitions;
                definitions.remove(&UDFFactory::get_udf_key(tenant, name));

                Ok(())
            }
            Err(lock_error) => Err(ErrorCode::RegisterUDFError(format!(
                "Can not unregister UDF: {}, error: {:?}",
                name, lock_error
            ))),
        }
    }

    pub fn get_definition(tenant: &str, name: &str) -> Result<Option<UDFDefinition>> {
        match UDF_FACTORY.lock() {
            Ok(factory) => match factory
                .definitions
                .get(&UDFFactory::get_udf_key(tenant, name))
            {
                Some(definition) => Ok(Some(definition.clone())),
                None => Ok(None),
            },
            Err(lock_error) => Err(ErrorCode::UnknownUDF(format!(
                "Can not get UDF: {}, error: {:?}",
                name, lock_error
            ))),
        }
    }

    fn get_udf_key(tenant: &str, name: &str) -> String {
        format!("{}/{}", tenant, name.to_lowercase())
    }
}
