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

use std::net::SocketAddr;

use common_exception::ErrorCode;
use common_exception::Result;
use serde::de::Error;
use serde::Deserializer;
use serde::Serializer;

#[derive(Clone, PartialEq, Debug)]
pub enum Address {
    SocketAddress(SocketAddr),
    Named((String, u16)),
}

impl Address {
    pub fn create(address: &str) -> Result<Address> {
        if let Ok(addr) = address.parse::<SocketAddr>() {
            return Ok(Address::SocketAddress(addr));
        }

        match address.find(':') {
            None => Err(ErrorCode::BadAddressFormat(format!(
                "Address must contain port, help: {}:port",
                address
            ))),
            Some(index) => {
                let (address, port) = address.split_at(index);
                let port = port.trim_start_matches(':').parse::<u16>().map_err(|_| {
                    ErrorCode::BadAddressFormat("The address port must between 0 and 65535")
                })?;

                Ok(Address::Named((address.to_string(), port)))
            }
        }
    }

    pub fn hostname(&self) -> String {
        match self {
            Self::SocketAddress(addr) => addr.ip().to_string(),
            Self::Named((hostname, _)) => hostname.clone(),
        }
    }

    pub fn port(&self) -> u16 {
        match self {
            Self::SocketAddress(addr) => addr.port(),
            Self::Named((_, port)) => *port,
        }
    }
}

impl ToString for Address {
    fn to_string(&self) -> String {
        match self {
            Self::SocketAddress(addr) => addr.to_string(),
            Self::Named((hostname, port)) => format!("{}:{}", hostname, port),
        }
    }
}

impl serde::Serialize for Address {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        String::deserialize(deserializer).and_then(|address| match Address::create(&address) {
            Ok(address) => Ok(address),
            Err(error_code) => Err(D::Error::custom(error_code)),
        })
    }
}
