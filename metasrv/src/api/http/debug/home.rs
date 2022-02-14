// Copyright 2021 Datafuse Labs.
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

use std::num::NonZeroI32;

use poem::web::Html;
use poem::IntoResponse;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PProfRequest {
    #[serde(default = "PProfRequest::default_seconds")]
    pub(crate) seconds: u64,
    #[serde(default = "PProfRequest::default_frequency")]
    pub(crate) frequency: NonZeroI32,
}

impl PProfRequest {
    pub(crate) fn default_seconds() -> u64 {
        5
    }
    pub(crate) fn default_frequency() -> NonZeroI32 {
        // safe unwrap
        NonZeroI32::new(99).unwrap()
    }
}

// return home page for default pprof results
#[poem::handler]
pub async fn debug_home_handler() -> impl IntoResponse {
    Html(format!(
        r#"<a href="/debug/pprof/profile?seconds={}">pprof/profile</a>"#,
        PProfRequest::default_seconds()
    ))
}
