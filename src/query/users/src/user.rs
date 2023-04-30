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

pub struct CertifiedInfo {
    pub user_name: String,
    pub user_password: Vec<u8>,
    pub user_client_address: String,
}

impl CertifiedInfo {
    pub fn create(user: &str, password: impl AsRef<[u8]>, address: &str) -> CertifiedInfo {
        CertifiedInfo {
            user_name: user.to_string(),
            user_password: password.as_ref().to_vec(),
            user_client_address: address.to_string(),
        }
    }
}
