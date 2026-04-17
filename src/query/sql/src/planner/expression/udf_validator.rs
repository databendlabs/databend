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

use std::collections::HashSet;

use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Lambda;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_functions::is_builtin_function;
use derive_visitor::Drive;
use derive_visitor::Visitor;

use crate::plans::UDFLanguage;

#[derive(Default, Visitor)]
#[visitor(ColumnRef(enter), FunctionCall(enter), Lambda(enter))]
pub struct UDFValidator {
    pub name: String,
    pub parameters: Vec<String>,
    pub lambda_parameters: Vec<String>,

    pub expr_params: HashSet<String>,
    pub has_recursive: bool,
}

impl UDFValidator {
    fn enter_column_ref(&mut self, column: &ColumnRef) {
        self.expr_params.insert(column.column.name().to_string());
    }

    fn enter_function_call(&mut self, func: &FunctionCall) {
        let name = &func.name.name;
        if !is_builtin_function(name) && self.name.eq_ignore_ascii_case(name) {
            self.has_recursive = true;
        }
    }

    fn enter_lambda(&mut self, lambda: &Lambda) {
        self.lambda_parameters
            .extend(lambda.params.iter().map(|v| v.name.clone()));
    }

    pub fn verify_definition_expr(&mut self, definition_expr: &Expr) -> Result<()> {
        self.expr_params.clear();

        definition_expr.drive(self);

        if self.has_recursive {
            return Err(ErrorCode::SyntaxException("Recursive UDF is not supported"));
        }
        let expr_params = &self.expr_params;
        let parameters = self
            .parameters
            .iter()
            .chain(self.lambda_parameters.iter())
            .cloned()
            .collect::<HashSet<_>>();

        let params_not_declared: HashSet<_> = expr_params.difference(&parameters).collect();
        let params_not_used: HashSet<_> = parameters.difference(expr_params).collect();

        if params_not_declared.is_empty() && params_not_used.is_empty() {
            return Ok(());
        }

        Err(ErrorCode::SyntaxException(format!(
            "{}{}",
            if params_not_declared.is_empty() {
                "".to_string()
            } else {
                format!("Parameters are not declared: {:?}", params_not_declared)
            },
            if params_not_used.is_empty() {
                "".to_string()
            } else {
                format!("Parameters are not used: {:?}", params_not_used)
            },
        )))
    }

    pub fn is_udf_script_allowed(lang: &UDFLanguage) -> Result<()> {
        match lang {
            UDFLanguage::JavaScript
                if GlobalConfig::instance().query.common.enable_udf_js_script =>
            {
                Ok(())
            }
            UDFLanguage::Python
                if GlobalConfig::instance()
                    .query
                    .common
                    .enable_udf_python_script
                    || GlobalConfig::instance().query.common.enable_udf_sandbox =>
            {
                Ok(())
            }
            UDFLanguage::WebAssembly
                if GlobalConfig::instance().query.common.enable_udf_wasm_script =>
            {
                Ok(())
            }
            other => Err(ErrorCode::Unimplemented(format!(
                "UDF {} script is not enabled in config",
                other
            ))),
        }
    }

    pub fn is_udf_cloud_script_allowed(lang: &UDFLanguage) -> Result<()> {
        if !GlobalConfig::instance().query.common.enable_udf_sandbox {
            return Err(ErrorCode::Unimplemented(
                "SandboxUDF is not enabled, you can enable it by setting 'enable_udf_sandbox = true' in query node config",
            ));
        }
        // TODO: more lang e.g. JavaScript ..
        if *lang != UDFLanguage::Python {
            return Err(ErrorCode::InvalidArgument(
                "SandboxUDF only supports python language",
            ));
        }
        Ok(())
    }

    pub fn is_udf_server_allowed(address: &str) -> Result<()> {
        if !GlobalConfig::instance().query.common.enable_udf_server {
            return Err(ErrorCode::Unimplemented(
                "UDF server is not allowed, you can enable it by setting 'enable_udf_server = true' in query node config",
            ));
        }

        let url_addr = url::Url::parse(address)
            .map_err_to_code(ErrorCode::InvalidArgument, || {
                format!("udf server address '{address}' is invalid, please check the address",)
            })?;

        let udf_server_allow_insecure = GlobalConfig::instance()
            .query
            .common
            .udf_server_allow_insecure;
        if !udf_server_allow_insecure && url_addr.scheme() != "https" {
            return Err(ErrorCode::Unimplemented(
                "Insecure UDF server is not allowed, you can enable it by setting 'udf_server_allow_insecure = true' in query node config",
            ));
        }

        let udf_server_allow_list = &GlobalConfig::instance().query.common.udf_server_allow_list;
        if udf_server_allow_list.iter().all(|allow_url| {
            if let Ok(allow_url) = url::Url::parse(allow_url) {
                allow_url.host_str() != url_addr.host_str()
            } else {
                true
            }
        }) {
            return Err(ErrorCode::InvalidArgument(format!(
                "Unallowed UDF server address, '{address}' is not in udf_server_allow_list"
            )));
        }
        Ok(())
    }
}
