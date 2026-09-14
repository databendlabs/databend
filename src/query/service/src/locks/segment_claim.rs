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

use async_trait::async_trait;
use databend_common_exception::Result;
use databend_common_meta_api::SegmentClaimApi;
use databend_common_meta_app::schema::DeleteSegmentClaimReq;
use databend_common_meta_app::schema::ExtendSegmentClaimReq;
use databend_common_users::UserApiProvider;

use crate::locks::lease_keeper::LeaseKeeper;
use crate::locks::lease_keeper::LeaseOps;
use crate::sessions::QueryContext;

struct SegmentClaimLeaseOps {
    extend_req: ExtendSegmentClaimReq,
    description: String,
}

#[async_trait]
impl LeaseOps for SegmentClaimLeaseOps {
    async fn extend(&self) -> Result<()> {
        UserApiProvider::instance()
            .get_meta_store_client()
            .extend_segment_claim(self.extend_req.clone())
            .await
            .map_err(Into::into)
    }

    async fn delete(&self) -> Result<()> {
        UserApiProvider::instance()
            .get_meta_store_client()
            .delete_segment_claim(DeleteSegmentClaimReq {
                tenant: self.extend_req.tenant.clone(),
                table_id: self.extend_req.table_id,
                claim_id: self.extend_req.claim_id,
            })
            .await
            .map_err(Into::into)
    }

    fn description(&self) -> &str {
        &self.description
    }
}

#[derive(Default)]
pub(super) struct SegmentClaimHolder {
    keeper: Arc<LeaseKeeper>,
}

impl SegmentClaimHolder {
    pub(super) fn start(
        self: &Arc<Self>,
        extend_req: ExtendSegmentClaimReq,
        ctx: Arc<QueryContext>,
    ) {
        // Segment claims have no queued acquisition phase. Meta has already selected this claim as
        // the winner and refreshed its full TTL before the holder starts maintaining it.
        let ttl = extend_req.ttl;
        let ops = SegmentClaimLeaseOps {
            description: format!(
                "segment claim for table {} claim {}",
                extend_req.table_id, extend_req.claim_id
            ),
            extend_req,
        };
        self.keeper.start(ttl, ops, move |error| {
            ctx.kill(error);
        });
    }

    pub(super) fn shutdown(&self) {
        self.keeper.shutdown();
    }
}
