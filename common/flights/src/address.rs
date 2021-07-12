// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

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
                    ErrorCode::BadAddressFormat(format!(
                        "The address '{}' port must between 0 and 65535",
                        address
                    ))
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
