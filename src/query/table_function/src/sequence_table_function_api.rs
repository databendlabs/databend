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
use databend_common_management::table_function::SequenceTableFunctionApiError;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::tenant::Tenant;

pub struct SequenceTableFunctionApi {}

impl SequenceTableFunctionApi {
    #[async_backtrace::framed]
    pub async fn exist_sequence(
        catalog: Arc<dyn Catalog>,
        tenant: Tenant,
        sequence_name: String,
    ) -> Result<bool, SequenceTableFunctionApiError> {
        let req = GetSequenceReq {
            ident: SequenceIdent::new(tenant, sequence_name),
        };

        let reply = catalog.get_sequence(req).await;
        Ok(reply.is_ok())
    }

    #[async_backtrace::framed]
    pub async fn get_sequence_nextval(
        catalog: Arc<dyn Catalog>,
        tenant: Tenant,
        sequence_name: String,
    ) -> Result<u64, SequenceTableFunctionApiError> {
        Ok(1024)
    }
}
