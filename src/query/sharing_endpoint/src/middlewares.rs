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

use base64::engine::general_purpose;
use base64::prelude::*;
use poem::Endpoint;
use poem::IntoResponse;
use poem::Middleware;
use poem::Request;
use poem::Response;
use poem::Result;

use crate::models::Credentials;

pub struct SharingAuth;

impl<E: Endpoint> Middleware<E> for SharingAuth {
    type Output = SharingAuthImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        SharingAuthImpl(ep)
    }
}

pub struct SharingAuthImpl<E>(E);

impl<E: Endpoint> Endpoint for SharingAuthImpl<E> {
    type Output = Response;

    // TODO(zhihanz) current implementation only used for stateless test
    // for production usage, we need to implement a middleware with JWT authentication
    #[async_backtrace::framed]
    async fn call(&self, mut req: Request) -> Result<Self::Output> {
        // decode auth header from bearer base64
        let auth_header = req
            .headers()
            .get("Authorization")
            .unwrap()
            .to_str()
            .unwrap();
        let auth_header = auth_header.split(' ').collect::<Vec<&str>>();
        let auth_header = auth_header[1];
        let auth_header = general_purpose::STANDARD.decode(auth_header).unwrap();
        let auth_header = String::from_utf8(auth_header).unwrap();
        req.extensions_mut()
            .insert(Credentials { token: auth_header });
        // add json content type if not provided
        if req.headers().get("Content-Type").is_none() {
            req.headers_mut()
                .insert("Content-Type", "application/json".parse().unwrap());
        }

        let res = self.0.call(req).await;
        match res {
            Ok(resp) => {
                let resp = resp.into_response();
                Ok(resp)
            }
            Err(err) => {
                // println!("err: {:?}", err);
                Err(err)
            }
        }
    }
}
