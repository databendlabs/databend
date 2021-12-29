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

use chrono_tz::Tz;

use crate::binary::ReadEx;
use crate::errors::DriverError;
use crate::errors::Error;
use crate::errors::Result;
use crate::protocols::HelloRequest;
use crate::protocols::Packet;
use crate::protocols::QueryRequest;
use crate::protocols::{self};
use crate::types::Block;

/// The internal clickhouse client request parser.
pub struct Parser<T> {
    reader: T,

    tz: Tz,
}

/// The parser can be used to parse clickhouse client requests
impl<'a, T: Read> Parser<T> {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub(crate) fn new(reader: T, tz: Tz) -> Parser<T> {
        Self { reader, tz }
    }

    pub(crate) fn parse_packet(
        &mut self,
        hello: &Option<HelloRequest>,
        compress: bool,
    ) -> Result<Packet> {
        let packet = self.reader.read_uvarint()?;

        match packet {
            protocols::CLIENT_PING => Ok(Packet::Ping),
            protocols::CLIENT_CANCEL => Ok(Packet::Cancel),
            protocols::CLIENT_DATA | protocols::CLIENT_SCALAR => {
                Ok(self.parse_data(packet == protocols::CLIENT_SCALAR, compress)?)
            }
            protocols::CLIENT_QUERY => Ok(self.parse_query(hello, compress)?),
            protocols::CLIENT_HELLO => Ok(self.parse_hello()?),

            _ => Err(Error::Driver(DriverError::UnknownPacket { packet })),
        }
    }

    fn parse_hello(&mut self) -> Result<Packet> {
        Ok(Packet::Hello(HelloRequest::read_from(&mut self.reader)?))
    }

    fn parse_query(&mut self, hello: &Option<HelloRequest>, _compress: bool) -> Result<Packet> {
        match hello {
            Some(ref hello) => {
                let query = QueryRequest::read_from(&mut self.reader, hello)?;
                Ok(Packet::Query(query))
            }
            _ => Err(Error::Driver(DriverError::UnexpectedPacket)),
        }
    }

    fn parse_data(&mut self, _scalar: bool, compress: bool) -> Result<Packet> {
        let _temporary_table = self.reader.read_string()?;
        let block = Block::load(&mut self.reader, self.tz, compress)?;
        Ok(Packet::Data(block))
    }
}
