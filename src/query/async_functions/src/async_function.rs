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

use std::sync::Arc;

use databend_common_ast::ast::Expr;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::Result;
use databend_common_exception::Span;
use databend_common_expression::types::DataType;
use databend_common_expression::Scalar;
use databend_common_meta_app::tenant::Tenant;
use educe::Educe;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct AsyncFunctionCall {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub func_name: String,
    pub display_name: String,
    pub return_type: Box<DataType>,
    pub arguments: Vec<String>,
    pub tenant: Tenant,
}

#[async_trait::async_trait]
pub trait AsyncFunction: Sync + Send {
    fn func_name(&self) -> String;

    async fn generate(
        &self,
        catalog: Arc<dyn Catalog>,
        async_func: &AsyncFunctionCall,
    ) -> Result<Scalar>;

    async fn resolve(
        &self,
        span: Span,
        tenant: Tenant,
        catalog: Arc<dyn Catalog>,
        arguments: &[&Expr],
    ) -> Result<AsyncFunctionCall>;
}
