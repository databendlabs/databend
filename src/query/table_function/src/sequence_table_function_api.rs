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
use databend_common_exception::Span;
use databend_common_management::table_function::SequenceTableFunctionApiError;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::tenant::Tenant;
use parking_lot::RwLock;

pub struct SequenceTableFunctionApi {
    catalog: Arc<dyn Catalog>,
}

impl SequenceTableFunctionApi {
    #[async_backtrace::framed]
    pub fn init(catalog: Arc<dyn Catalog>) {
        let table_function_api = SequenceTableFunctionApi { catalog };
        GlobalInstance::set(Arc::new(table_function_api));
    }

    pub fn instance() -> Arc<SequenceTableFunctionApi> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    pub async fn get_sequence(
        &self,
        tenant: Tenant,
        sequence_name: String,
    ) -> Result<bool, SequenceTableFunctionApiError> {
        let req = GetSequenceReq {
            ident: SequenceIdent::new(tenant, sequence_name),
        };

        if let Ok(_reply) = self.catalog.get_sequence(req).await {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
