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

use common_base::tokio::io::AsyncReadExt;
use common_base::tokio::io::AsyncWriteExt;
use common_base::tokio::net::TcpStream;
use common_exception::Result;
use msql_srv::ErrorKind;

pub struct RejectConnection;

impl RejectConnection {
    pub async fn reject_mysql_connection(
        mut stream: TcpStream,
        code: ErrorKind,
        error_message: impl Into<String>,
    ) -> Result<()> {
        RejectConnection::send_handshake(&mut stream).await?;
        RejectConnection::receive_handshake_response(&mut stream).await?;

        // Send error. Packet[seq = 2]
        let mut buffer = vec![0xFF_u8];
        buffer.extend(&(code as u16).to_le_bytes());
        buffer.extend(&vec![b'#']);
        buffer.extend(code.sqlstate());
        buffer.extend(error_message.into().as_bytes());

        let size = buffer.len().to_le_bytes();
        buffer.splice(0..0, [size[0], size[1], size[2], 2].iter().cloned());
        stream.write_all(&buffer).await?;
        stream.flush().await?;

        Ok(())
    }

    async fn send_handshake(stream: &mut TcpStream) -> Result<()> {
        // Send handshake, packet from msql-srv. Packet[seq = 0]
        stream
            .write_all(&[
                69, 00, 00, 00, 10, 53, 46, 49, 46, 49, 48, 45, 97, 108, 112, 104, 97, 45, 109,
                115, 113, 108, 45, 112, 114, 111, 120, 121, 0, 8, 0, 0, 0, 59, 88, 44, 112, 111,
                95, 107, 125, 0, 0, 66, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 111,
                54, 94, 87, 122, 33, 47, 107, 77, 125, 78, 0,
            ])
            .await?;

        stream.flush().await?;

        Ok(())
    }

    async fn receive_handshake_response(stream: &mut TcpStream) -> Result<()> {
        let mut buffer = vec![0; 4];
        stream.read_exact(&mut buffer).await?;

        // Ignore handshake response. Packet[seq = 1]
        let len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], 0]);
        buffer.resize(len as usize, 0);
        stream.read_exact(&mut buffer).await?;

        Ok(())
    }
}
