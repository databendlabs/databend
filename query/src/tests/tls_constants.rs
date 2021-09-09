// Copyright 2020 Datafuse Labs.
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
//

pub const TEST_CA_CERT: &'static str = "../tests/certs/ca.pem";
pub const TEST_SERVER_CERT: &'static str = "../tests/certs/server.pem";
pub const TEST_SERVER_KEY: &'static str = "../tests/certs/server.key";
pub const TEST_CN_NAME: &'static str = "localhost";

pub const TEST_TLS_CA_CERT: &'static str = "../tools/tls/cfssl/ca/ca.pem";
pub const TEST_TLS_SERVER_CERT: &'static str = "../tools/tls/cfssl/server/server.pem";
pub const TEST_TLS_SERVER_KEY: &'static str = "../tools/tls/cfssl/server/pkcs8-server-key.pem";
// pub const TEST_TLS_CLIENT_CERT: &'static str = "../tools/tls/cfssl/client/client.pem";
// pub const TEST_TLS_CLIENT_KEY: &'static str = "../tools/tls/cfssl/client/pkcs8-client-key.pem";
pub const TEST_TLS_CLIENT_IDENTITY: &'static str = "../tools/tls/cfssl/client/client-identity.pfx";
pub const TEST_TLS_CLIENT_PASSWORD: &'static str = "datafuse";
