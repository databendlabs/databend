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

use databend_common_exception::Result;
use databend_common_meta_app::principal::procedure::ProcedureInfo;
use databend_common_meta_app::principal::CreateProcedureReq;
use databend_common_meta_app::principal::DropProcedureReq;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::tenant::Tenant;

use crate::UserApiProvider;

/// Procedure operations.
impl UserApiProvider {
    // Add a new Procedure.
    #[async_backtrace::framed]
    pub async fn add_procedure(&self, tenant: &Tenant, req: CreateProcedureReq) -> Result<()> {
        let procedure_api = self.procedure_api(tenant);
        procedure_api.create_procedure(req).await?;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn get_procedure(
        &self,
        tenant: &Tenant,
        req: GetProcedureReq,
    ) -> Result<ProcedureInfo> {
        let procedure_api = self.procedure_api(tenant);
        let procedure = procedure_api.get_procedure(req).await?;

        Ok(procedure)
    }

    // Drop a Procedure by name.
    #[async_backtrace::framed]
    pub async fn drop_procedure(&self, tenant: &Tenant, req: DropProcedureReq) -> Result<()> {
        let _ = self.procedure_api(tenant).drop_procedure(req).await?;
        Ok(())
    }
}
