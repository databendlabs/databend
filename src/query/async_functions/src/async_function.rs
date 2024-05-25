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
use databend_common_ast::Span;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Scalar;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::tenant::Tenant;
use educe::Educe;

use crate::sequence_async_function::SequenceAsyncFunction;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub enum AsyncFunction {
    SequenceAsyncFunction(SequenceAsyncFunction),
}

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
    pub function: AsyncFunction,
}

impl AsyncFunction {
    pub async fn generate(
        &self,
        catalog: Arc<dyn Catalog>,
        async_func: &AsyncFunctionCall,
    ) -> Result<Scalar> {
        match &async_func.function {
            AsyncFunction::SequenceAsyncFunction(async_function) => {
                async_function.generate(catalog, async_func).await
            }
        }
    }
}

pub async fn resolve_async_function(
    span: Span,
    tenant: Tenant,
    catalog: Arc<dyn Catalog>,
    func_name: &str,
    arguments: &[&Expr],
) -> Result<AsyncFunctionCall> {
    if func_name == "nextval" {
        resolve_nextval(span, tenant, catalog, arguments).await
    } else {
        Err(ErrorCode::SemanticError(format!(
            "cannot find function {}",
            func_name
        )))
    }
}

async fn resolve_nextval(
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
        func_name: "nextval".to_string(),
        display_name: format!("nextval({})", sequence_name),
        return_type: Box::new(DataType::Number(NumberDataType::UInt64)),
        arguments: vec![sequence_name],
        tenant,
        function: AsyncFunction::SequenceAsyncFunction(SequenceAsyncFunction {}),
    };

    Ok(table_func)
}
