// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use common_meta_types::UserInfo;
use poem::FromRequest;
use poem::Request;
use poem::RequestBody;
use poem::Result as PoemResult;

use crate::sessions::SessionManager;
use crate::sessions::SessionRef;
use crate::sessions::SessionType;

pub struct HttpQueryContext {
    pub session_mgr: Arc<SessionManager>,
    pub user_info: UserInfo,
    pub tenant_id: Option<String>,
}

impl HttpQueryContext {
    pub async fn create_session(&self, typ: SessionType) -> Result<SessionRef> {
        let session = self.session_mgr.create_session(typ).await?;
        session.set_current_user(self.user_info.clone());
        if let Some(tenant_id) = self.tenant_id.clone() {
            session.set_current_tenant(tenant_id);
        }
        Ok(session)
    }
}

#[async_trait::async_trait]
impl<'a> FromRequest<'a> for &'a HttpQueryContext {
    async fn from_request(req: &'a Request, _body: &mut RequestBody) -> PoemResult<Self> {
        Ok(req.extensions().get::<HttpQueryContext>().expect(
            "To use the `HttpQueryContext` extractor, the `HTTPSessionMiddleware` is required",
        ))
    }
}
