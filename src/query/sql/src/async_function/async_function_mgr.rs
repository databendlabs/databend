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
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_meta_app::tenant::Tenant;

use super::sequence_async_function::SequenceAsyncFunction;
use crate::plans::AsyncFunctionCall;
use crate::AsyncFunction;
use crate::ScalarExpr;

pub struct AsyncFunctionManager {
    functions: HashMap<String, Arc<dyn AsyncFunction>>,
}

impl AsyncFunctionManager {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Self::create());
        Ok(())
    }

    pub fn instance() -> Arc<Self> {
        GlobalInstance::get::<Arc<Self>>()
    }

    pub fn create() -> Arc<AsyncFunctionManager> {
        let mut functions = HashMap::new();
        functions.insert("nextval".to_string(), SequenceAsyncFunction::create());

        Arc::new(AsyncFunctionManager { functions })
    }

    pub async fn generate(
        &self,
        tenant: Tenant,
        catalog: Arc<dyn Catalog>,
        async_func: &AsyncFunctionCall,
    ) -> Result<ScalarExpr> {
        if let Some(func) = self.functions.get(&async_func.func_name) {
            func.generate(tenant, catalog, async_func).await
        } else {
            Err(ErrorCode::SemanticError(format!(
                "cannot find function {}",
                async_func.func_name
            )))
        }
    }

    pub async fn resolve(
        &self,
        tenant: Tenant,
        catalog: Arc<dyn Catalog>,
        func_name: &str,
        arguments: &[String],
    ) -> Result<DataType> {
        if let Some(func) = self.functions.get(func_name) {
            func.resolve(tenant, catalog, arguments).await
        } else {
            Err(ErrorCode::SemanticError(format!(
                "cannot find function {}",
                func_name
            )))
        }
    }
}
