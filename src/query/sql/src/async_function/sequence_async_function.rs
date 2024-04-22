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

use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::tenant::Tenant;

use crate::plans::AsyncFunctionCall;
use crate::plans::ConstantExpr;
use crate::AsyncFunction;
use crate::ScalarExpr;

pub struct SequenceAsyncFunction {
    return_type: DataType,
}

impl SequenceAsyncFunction {
    pub fn create() -> Arc<dyn AsyncFunction> {
        Arc::new(SequenceAsyncFunction {
            return_type: DataType::Number(NumberDataType::UInt64),
        })
    }
}

#[async_trait::async_trait]
impl AsyncFunction for SequenceAsyncFunction {
    fn function_name(&self) -> &str {
        "nextval"
    }

    async fn resolve(
        &self,
        tenant: Tenant,
        catalog: Arc<dyn Catalog>,
        arguments: &[String],
    ) -> Result<DataType> {
        if arguments.len() != 1 {
            return Err(ErrorCode::SemanticError(
                "nextval function need only one argument".to_string(),
            ));
        }
        let req = GetSequenceReq {
            ident: SequenceIdent::new(tenant, arguments[0].clone()),
        };

        let _ = catalog.get_sequence(req).await?;
        Ok(self.return_type.clone())
    }

    async fn generate(
        &self,
        tenant: Tenant,
        catalog: Arc<dyn Catalog>,
        async_func: &AsyncFunctionCall,
    ) -> Result<ScalarExpr> {
        let req = GetSequenceNextValueReq {
            ident: SequenceIdent::new(tenant, async_func.arguments[0].clone()),
            count: 1,
        };

        let reply = catalog.get_sequence_next_value(req).await?;
        let expr = ConstantExpr {
            span: async_func.span,
            value: Scalar::Number(NumberScalar::UInt64(reply.start)),
        };
        Ok(ScalarExpr::ConstantExpr(expr))
    }
}
