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

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::Expr;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::Span;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::tenant::Tenant;

use crate::AsyncFunction;
use crate::AsyncFunctionCall;

pub struct SequenceAsyncFunction {
    func_name: String,
    return_type: DataType,
}

impl SequenceAsyncFunction {
    pub fn create() -> Arc<dyn AsyncFunction> {
        Arc::new(SequenceAsyncFunction {
            func_name: "nextval".to_string(),
            return_type: DataType::Number(NumberDataType::UInt64),
        })
    }
}

#[async_trait::async_trait]
impl AsyncFunction for SequenceAsyncFunction {
    fn func_name(&self) -> String {
        self.func_name.clone()
    }

    async fn resolve(
        &self,
        span: Span,
        tenant: Tenant,
        catalog: Arc<dyn Catalog>,
        arguments: &[&Expr],
    ) -> Result<AsyncFunctionCall> {
        if arguments.len() != 1 {
            return Err(ErrorCode::SemanticError(format!(
                "nextval function need one argument but got {}",
                arguments.len()
            )));
        }
        let sequence_name = if let Expr::ColumnRef { column, .. } = arguments[0] {
            if let ColumnID::Name(name) = &column.column {
                name.name.clone()
            } else {
                return Err(ErrorCode::SemanticError(
                    "async function can only used as column".to_string(),
                ));
            }
        } else {
            return Err(ErrorCode::SemanticError(
                "async function can only used as column".to_string(),
            ));
        };

        let req = GetSequenceReq {
            ident: SequenceIdent::new(tenant.clone(), sequence_name.clone()),
        };

        let _ = catalog.get_sequence(req).await?;

        let table_func = AsyncFunctionCall {
            span,
            func_name: self.func_name.clone(),
            display_name: format!("nextval({})", sequence_name),
            return_type: Box::new(self.return_type.clone()),
            arguments: vec![sequence_name],
            tenant,
        };

        Ok(table_func)
    }

    async fn generate(
        &self,
        catalog: Arc<dyn Catalog>,
        async_func: &AsyncFunctionCall,
    ) -> Result<Scalar> {
        let tenant = &async_func.tenant;
        let req = GetSequenceNextValueReq {
            ident: SequenceIdent::new(tenant, async_func.arguments[0].clone()),
            count: 1,
        };

        let reply = catalog.get_sequence_next_value(req).await?;

        Ok(Scalar::Number(NumberScalar::UInt64(reply.start)))
    }
}
