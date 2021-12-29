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

use std::io::Read;

use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::error_codes;
use crate::errors::Error;
use crate::errors::Result;
use crate::errors::ServerError;
use crate::protocols::*;

#[derive(Default, Debug, Clone)]
pub struct HelloRequest {
    pub client_name: Vec<u8>,
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_revision: u64,
    pub default_database: Vec<u8>,

    pub user: String,
    pub password: Vec<u8>,

    // Not set currently
    pub client_version_patch: u64,
}

impl HelloRequest {
    pub fn read_from<R: Read>(reader: &mut R) -> Result<HelloRequest> {
        let request = HelloRequest {
            client_name: reader.read_len_encode_bytes()?,
            client_version_major: reader.read_uvarint()?,
            client_version_minor: reader.read_uvarint()?,
            client_revision: reader.read_uvarint()?,
            default_database: reader.read_len_encode_bytes()?,
            user: reader.read_string()?,
            password: reader.read_len_encode_bytes()?,

            client_version_patch: 0,
        };

        if request.user.is_empty() {
            return Err(Error::Server(ServerError {
                name: "UNEXPECTED_PACKET_FROM_CLIENT".to_string(),
                code: error_codes::UNEXPECTED_PACKET_FROM_CLIENT,
                message: "Unexpected packet from client (no user in Hello package)".to_string(),
                stack_trace: "".to_string(),
            }));
        }

        Ok(request)
    }
}

pub struct HelloResponse {
    pub dbms_name: String,
    pub dbms_version_major: u64,
    pub dbms_version_minor: u64,
    pub dbms_tcp_protocol_version: u64,
    pub timezone: String,
    pub server_display_name: String,
    pub dbms_version_patch: u64,
}

impl HelloResponse {
    pub fn encode(&self, encoder: &mut Encoder, client_revision: u64) -> Result<()> {
        encoder.uvarint(SERVER_HELLO);

        encoder.string(&self.dbms_name);
        encoder.uvarint(self.dbms_version_major);
        encoder.uvarint(self.dbms_version_minor);
        encoder.uvarint(self.dbms_tcp_protocol_version);

        if client_revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
            encoder.string(&self.timezone);
        }

        if client_revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
            encoder.string(&self.server_display_name);
        }

        if client_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            encoder.uvarint(self.dbms_version_patch);
        }

        Ok(())
    }
}
