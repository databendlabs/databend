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

use aggregating_index::get_agg_index_handler;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CreateIndexReq;
use common_meta_app::schema::DropIndexReq;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::IndexNameIdent;
use common_sql::plans::SetVectorIndexParamPlan;
use common_storages_fuse::TableContext;
use common_vector::index::ParamKind;
use common_vector::index::VectorIndex;

use super::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct SetVectorIndexParamInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetVectorIndexParamPlan,
}

impl SetVectorIndexParamInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetVectorIndexParamPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetVectorIndexParamInterpreter {
    fn name(&self) -> &str {
        "SetVectorIndexParaInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let index_name = &self.plan.index_name;
        let catalog = self.ctx.get_catalog(&self.ctx.get_current_catalog())?;
        let handler = get_agg_index_handler();

        let drop_index_req = DropIndexReq {
            if_exists: false,
            name_ident: IndexNameIdent {
                tenant: tenant.clone(),
                index_name: index_name.clone(),
            },
        };
        let _ = handler
            .do_drop_index(catalog.clone(), drop_index_req)
            .await?;

        let mut vector_index = self.plan.index_meta.vector_index.clone().unwrap();
        match vector_index {
            VectorIndex::IvfFlat(ref mut flat_index) => match self.plan.param_kind {
                ParamKind::NPROBE => {
                    let val = self
                        .plan
                        .val
                        .as_number()
                        .and_then(|x| x.try_get_uint())
                        .ok_or(ErrorCode::IllegalDataType(format!(
                            "unsupported param type in ivfflat index: {:?}",
                            self.plan.val
                        )))?;
                    flat_index.nprobe = val as usize;
                }
                _ => {
                    return Err(ErrorCode::StorageOther(
                        "unsupported param kind in ivfflat index".to_string(),
                    ));
                }
            },
        }

        let create_index_req = CreateIndexReq {
            if_not_exists: false,
            name_ident: IndexNameIdent {
                tenant,
                index_name: index_name.clone(),
            },
            meta: IndexMeta {
                vector_index: Some(vector_index),
                ..self.plan.index_meta.clone()
            },
        };
        let _ = handler.do_create_index(catalog, create_index_req).await?;
        Ok(PipelineBuildResult::create())
    }
}
