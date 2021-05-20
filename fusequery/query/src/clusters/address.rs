use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4, SocketAddrV6};
use common_exception::ErrorCodes;
use common_exception::Result;

#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize, Debug)]
pub enum Address {
    SocketAddress(SocketAddr),
    Named((String, u16)),
}

impl Address {
    pub fn create(address: &String) -> Result<Address> {
        if let Ok(addr) = address.parse::<SocketAddr>() {
            return Ok(Address::SocketAddress(addr));
        }

        match address.find(":") {
            None => Err(ErrorCodes::BadAddressFormat(format!("Address must contain port, help: {}:port", address))),
            Some(index) => {
                let (address, port) = address.split_at(index);
                let port = port.trim_start_matches(":")
                    .parse::<u16>()
                    .map_err(|error| {
                        ErrorCodes::BadAddressFormat("The address port must between 0 and 65535")
                    })?;

                Ok(Address::Named((address.to_string(), port)))
            }
        }
    }

    pub fn hostname(&self) -> String {
        match self {
            Self::SocketAddress(addr) => addr.ip().to_string(),
            Self::Named((hostname, _)) => hostname.clone()
        }
    }

    pub fn port(&self) -> u16 {
        match self {
            Self::SocketAddress(addr) => addr.port(),
            Self::Named((_, port)) => port.clone()
        }
    }
}
