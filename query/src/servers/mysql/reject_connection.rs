// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_runtime::tokio::io::AsyncReadExt;
use common_runtime::tokio::io::AsyncWriteExt;
use common_runtime::tokio::net::TcpStream;
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
        stream.write(&buffer).await?;
        stream.flush().await?;

        Ok(())
    }

    async fn send_handshake(stream: &mut TcpStream) -> Result<()> {
        // Send handshake, packet from msql-srv. Packet[seq = 0]
        stream
            .write(&[
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
        stream.read(&mut buffer).await?;

        // Ignore handshake response. Packet[seq = 1]
        let len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], 0]);
        buffer.resize(len as usize, 0);
        stream.read(&mut buffer).await?;

        Ok(())
    }
}
