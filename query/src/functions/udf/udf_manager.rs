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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::scalars::FunctionFactory;
use common_infallible::RwLock;
use common_meta_types::UserDefinedFunction;
use sqlparser::ast::Expr;

use crate::configs::Config;
use crate::functions::udf::UDFParser;
use crate::users::UserApiProvider;

pub struct UDFManager {
    pub(crate) conf: Config,
    user_api: Arc<UserApiProvider>,
    definitions: RwLock<HashMap<String, Expr>>,
}

impl UDFManager {
    pub fn create_global(conf: Config, user_api: Arc<UserApiProvider>) -> Self {
        UDFManager {
            conf,
            user_api,
            definitions: Default::default(),
        }
    }

    fn add_cache(&self, name: &str, definition: &str) -> Result<()> {
        let tenant = &self.conf.query.tenant_id;

        if Self::is_builtin_function(name) {
            return Err(ErrorCode::RegisterUDFError(format!(
                "Can not register builtin functions: {} - {}",
                name, definition
            )));
        }

        let mut lock = self.definitions.write();
        if lock.contains_key(name) {
            return Err(ErrorCode::RegisterUDFError(format!(
                "UDF: {} already exists",
                name
            )));
        } else {
            let mut udf_parser = UDFParser::default();
            let expr = udf_parser.parse_definition(definition)?;
            lock.insert(Self::get_udf_key(tenant, name), expr);
        }
        Ok(())
    }

    fn remove_cache(&self, name: &str) -> Result<()> {
        let tenant = &self.conf.query.tenant_id;

        let mut lock = self.definitions.write();
        lock.remove(&*Self::get_udf_key(tenant, name));
        Ok(())
    }

    fn get_cache(&self, name: &str) -> Result<Option<Expr>> {
        let lock = self.definitions.read();
        match lock.get(name) {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }

    pub async fn add(&self, name: &str, definition: &str) -> Result<()> {
        // Add to factory cache.
        self.add_cache(name, definition)?;

        // Add to metasrv.
        let udf = UserDefinedFunction::new(name, definition, "");
        let _ = self.user_api.add_udf(udf).await?;
        Ok(())
    }

    pub async fn drop(&self, name: &str, if_exists: bool) -> Result<()> {
        self.remove_cache(name)?;

        // Drop from metasrv.
        self.user_api.drop_udf(name, if_exists).await
    }

    pub async fn get(&self, name: &str) -> Result<Option<Expr>> {
        let cache_result = self.get_cache(name)?;
        match cache_result {
            x => return Ok(x),
            None => {
                let x = self.user_api.get_udf(name).await?;
                self.add_cache(name, &x.definition)?;
            }
        }
        self.get_cache(name)
    }

    fn is_builtin_function(name: &str) -> bool {
        FunctionFactory::instance().check(name) || AggregateFunctionFactory::instance().check(name)
    }

    fn get_udf_key(tenant: &str, name: &str) -> String {
        format!("{}/{}", tenant, name.to_lowercase())
    }
}
