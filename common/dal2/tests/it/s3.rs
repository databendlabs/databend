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

use common_dal2::credential::Credential;
use common_dal2::services::s3;

#[tokio::test]
async fn builder() {
    let mut builder = s3::Backend::build();

    let _ = builder
        .root("/path-to-file")
        .bucket("test-bucket")
        .region("us-east-1")
        .credential(Credential::hmac("access-key", "secret-key"))
        .endpoint("http://localhost:9000")
        .disable_ssl()
        .enable_path_style()
        .enable_signature_v2()
        .finish()
        .await
        .unwrap();

    ();
}
