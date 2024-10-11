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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::kv_app_error::KVAppError;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::principal::CreateProcedureReply;
use databend_common_meta_app::principal::CreateProcedureReq;
use databend_common_meta_app::principal::DropProcedureReq;
use databend_common_meta_app::principal::GetProcedureReply;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::tenant::Tenant;

use crate::UserApiProvider;

/// Procedure operations.
impl UserApiProvider {
    // Add a new Procedure.
    #[async_backtrace::framed]
    pub async fn add_procedure(
        &self,
        tenant: &Tenant,
        req: CreateProcedureReq,
        overriding: bool,
    ) -> Result<std::result::Result<CreateProcedureReply, KVAppError>> {
        let procedure_api = self.procedure_api(tenant);
        let replay = procedure_api.create_procedure(req, overriding).await;

        Ok(replay)
    }

    #[async_backtrace::framed]
    pub async fn get_procedure(
        &self,
        tenant: &Tenant,
        req: GetProcedureReq,
    ) -> Result<GetProcedureReply> {
        let procedure_api = self.procedure_api(tenant);
        let procedure = procedure_api
            .get_procedure(&req)
            .await?
            .ok_or_else(|| AppError::from(req.inner.unknown_error("get_procedure")))?;
        Ok(procedure)
    }

    // Drop a Procedure by name.
    #[async_backtrace::framed]
    pub async fn drop_procedure(
        &self,
        tenant: &Tenant,
        req: DropProcedureReq,
        if_exists: bool,
    ) -> Result<()> {
        let dropped = self
            .procedure_api(tenant)
            .drop_procedure(&req.name_ident)
            .await?;
        if dropped.is_none() && !if_exists {
            return Err(ErrorCode::UnknownProcedure(format!(
                "Unknown procedure '{}' while drop procedure",
                req.name_ident
            )));
        }
        Ok(())
    }
}
