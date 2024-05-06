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
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::SequenceIdent;
use educe::Educe;

use crate::AsyncFunctionCall;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct SequenceAsyncFunction {}

impl SequenceAsyncFunction {
    pub async fn generate(
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
