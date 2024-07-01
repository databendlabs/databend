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

use std::collections::HashMap;

use databend_common_ast::ast::Statement;
use databend_common_ast::ast::UDFDefinition;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_config::UDFConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_sql::resolve_type_name;
use log::warn;

pub struct BuiltinUDFs {
    udf_configs: Vec<UDFConfig>,
}

impl BuiltinUDFs {
    pub fn create(udf_configs: Vec<UDFConfig>) -> BuiltinUDFs {
        BuiltinUDFs { udf_configs }
    }

    // Parse the UDF definition and return the UserDefinedFunction.
    async fn parse_udf_definition(
        &self,
        name: &str,
        definition: &str,
    ) -> Result<UserDefinedFunction> {
        let tokens = tokenize_sql(definition)?;
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL)?;

        match stmt {
            Statement::CreateUDF(ast) => match ast.definition {
                UDFDefinition::UDFServer {
                    arg_types,
                    return_type,
                    address,
                    handler,
                    language,
                } => {
                    let mut arg_datatypes = Vec::with_capacity(arg_types.len());
                    for arg_type in arg_types {
                        arg_datatypes.push(DataType::from(&resolve_type_name(&arg_type, true)?));
                    }
                    let return_type = DataType::from(&resolve_type_name(&return_type, true)?);
                    let udf = UserDefinedFunction::create_udf_server(
                        name,
                        &address,
                        &handler,
                        &language,
                        arg_datatypes,
                        return_type,
                        "Built-in UDF",
                    );

                    Ok(udf)
                }
                _ => Err(ErrorCode::SyntaxException(format!(
                    "Invalid built-in UDF definition: '{}', expected UDFServer but got: {:?}",
                    definition, ast
                ))),
            },
            _ => Err(ErrorCode::SyntaxException(format!(
                "Invalid built-in UDF definition: '{}', expected CreateUDF but got: {:?}",
                definition, stmt
            ))),
        }
    }

    pub async fn to_meta_udfs(&self) -> Result<HashMap<String, UserDefinedFunction>> {
        let mut udf_map = HashMap::new();

        for udf_config in self.udf_configs.iter() {
            match self
                .parse_udf_definition(&udf_config.name, &udf_config.definition)
                .await
            {
                Ok(user_defined_function) => {
                    udf_map.insert(user_defined_function.name.clone(), user_defined_function);
                }
                Err(e) => {
                    warn!(
                        "Failed to parse UDF definition for '{}': {}",
                        udf_config.name, e
                    );
                }
            }
        }

        Ok(udf_map)
    }
}

#[cfg(test)]
mod tests {
    use tokio::test;

    use super::*;

    #[test]
    async fn test_to_meta_udfs() {
        // Array of mock data for different test cases
        let udf_configs = vec![
            UDFConfig {
                name: "test_udf1".to_string(),
                definition: "CREATE OR REPLACE FUNCTION test_udf1(STRING)
    RETURNS STRING
    LANGUAGE python
HANDLER = 'test_udf1'
ADDRESS = 'https://databend.com'"
                    .to_string(),
            },
            UDFConfig {
                name: "test_udf2".to_string(),
                definition: "CREATE OR REPLACE FUNCTION test_udf2(ARRAY(FLOAT), ARRAY(FLOAT))
    RETURNS FLOAT
    LANGUAGE python
HANDLER = 'test_udf2'
ADDRESS = 'https://databend.com'"
                    .to_string(),
            },
            UDFConfig {
                name: "invalid_udf".to_string(),
                definition: "SELECT 1".to_string(),
            },
        ];

        let builtin_udfs = BuiltinUDFs::create(udf_configs);

        let result = builtin_udfs.to_meta_udfs().await;

        // Verify the result
        match result {
            Ok(udf_map) => {
                // Test first UDF
                assert!(udf_map.contains_key("test_udf1"));
                let udf1 = udf_map.get("test_udf1").unwrap();
                assert_eq!(udf1.name, "test_udf1");

                // Test second UDF
                assert!(udf_map.contains_key("test_udf2"));
                let udf2 = udf_map.get("test_udf2").unwrap();
                assert_eq!(udf2.name, "test_udf2");

                // Test invalid UDF is not included
                assert!(!udf_map.contains_key("invalid_udf"));
            }
            Err(e) => {
                panic!("Unexpected error: {}", e);
            }
        }
    }
}
