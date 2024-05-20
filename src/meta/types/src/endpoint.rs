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

use std::fmt;

use anyerror::AnyError;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct Endpoint {
    addr: String,
    port: u16,
}

impl Endpoint {
    pub fn new(addr: impl ToString, port: u16) -> Self {
        Self {
            addr: addr.to_string(),
            port,
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Parse `1.2.3.4:5555` into `Endpoint`.
    pub fn parse(address: &str) -> Result<Self, AnyError> {
        let x = address.splitn(2, ':').collect::<Vec<_>>();
        if x.len() != 2 {
            return Err(AnyError::error(format!(
                "Failed to parse address: {}",
                address
            )));
        }
        let port = x[1].parse::<u16>().map_err(|e| {
            AnyError::error(format!("Failed to parse port: {}; address: {}", e, address))
        })?;
        Ok(Self::new(x[0], port))
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.addr, self.port)
    }
}

#[cfg(test)]
mod tests {
    use crate::Endpoint;

    #[test]
    fn test_endpoint_parse() -> anyhow::Result<()> {
        assert!(Endpoint::parse("1.2.3.4").is_err());
        assert!(Endpoint::parse("1.2.3.4:88888").is_err());

        assert_eq!("1.2.3.4", Endpoint::parse("1.2.3.4:1234")?.addr());
        assert_eq!(1234, Endpoint::parse("1.2.3.4:1234")?.port());

        Ok(())
    }
}
