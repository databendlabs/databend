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

use http::StatusCode;
use poem::FromRequest;
use poem::Request;
use poem::RequestBody;
use poem::Result as PoemResult;

use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

pub struct HttpQueryContext {
    session: Arc<Session>,
    pub query_id: String,
    pub node_id: String,
    pub deduplicate_label: Option<String>,
    pub user_agent: Option<String>,
    pub http_method: String,
    pub uri: String,
}

impl HttpQueryContext {
    pub fn new(
        session: Arc<Session>,
        query_id: String,
        node_id: String,
        deduplicate_label: Option<String>,
        user_agent: Option<String>,
        http_method: String,
        uri: String,
    ) -> Self {
        HttpQueryContext {
            session,
            query_id,
            node_id,
            deduplicate_label,
            user_agent,
            http_method,
            uri,
        }
    }

    pub fn upgrade_session(&self, session_type: SessionType) -> Result<Arc<Session>, poem::Error> {
        SessionManager::instance()
            .try_upgrade_session(self.session.clone(), session_type.clone())
            .map_err(|err| {
                poem::Error::from_string(err.message(), StatusCode::TOO_MANY_REQUESTS)
            })?;
        Ok(self.session.clone())
    }
}

#[async_trait::async_trait]
impl<'a> FromRequest<'a> for &'a HttpQueryContext {
    #[async_backtrace::framed]
    async fn from_request(req: &'a Request, _body: &mut RequestBody) -> PoemResult<Self> {
        Ok(req.extensions().get::<HttpQueryContext>().expect(
            "To use the `HttpQueryContext` extractor, the `HTTPSessionMiddleware` is required",
        ))
    }
}
