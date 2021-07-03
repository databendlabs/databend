// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::{Result, ErrorCode};
use common_runtime::tokio::io::AsyncReadExt;
use common_runtime::tokio::io::AsyncWriteExt;
use common_runtime::tokio::net::TcpStream;
use prost::bytes::BytesMut;

pub struct RejectCHConnection;

impl RejectCHConnection {
    pub async fn reject(mut stream: TcpStream, error: ErrorCode) -> Result<()> {
        RejectCHConnection::receive_hello(&mut stream).await?;
        let exception_name = String::from("NO_FREE_CONNECTION");
        RejectCHConnection::write_var_uint(&mut stream, 2).await?;
        stream.write_u32(203).await?;
        RejectCHConnection::write_binary_string(&mut stream, exception_name).await?;
        RejectCHConnection::write_binary_string(&mut stream, error.message()).await?;
        RejectCHConnection::write_binary_string(&mut stream, String::from("")).await?;
        stream.write_u8(false as u8).await?;
        Ok(())
    }

    async fn receive_hello(stream: &mut TcpStream) -> Result<()> {
        RejectCHConnection::read_var_uint(stream).await?;
        RejectCHConnection::read_binary_string(stream).await?;
        RejectCHConnection::read_var_uint(stream).await?;
        RejectCHConnection::read_var_uint(stream).await?;
        RejectCHConnection::read_var_uint(stream).await?;
        RejectCHConnection::read_binary_string(stream).await?;
        RejectCHConnection::read_binary_string(stream).await?;
        RejectCHConnection::read_binary_string(stream).await?;
        Ok(())
    }

    async fn read_var_uint(stream: &mut TcpStream) -> Result<u64> {
        let mut x = 0_u64;
        for index in 0..8 {
            let byte = stream.read_u8().await? as u64;
            x |= (byte & 0x7F) << (7 * index);
            if (byte & 0x80) == 0 {
                break;
            }
        }

        Ok(x)
    }

    async fn read_binary_string(stream: &mut TcpStream) -> Result<String> {
        let length = RejectCHConnection::read_var_uint(stream).await?;
        let mut binary_buffer = BytesMut::with_capacity(length as usize);
        stream.read_buf(&mut binary_buffer).await?;
        Ok(String::from_utf8(binary_buffer.to_vec())?)
    }

    async fn write_var_uint(stream: &mut TcpStream, value: u64) -> Result<()> {
        let mut value = value;
        for _index in 0..8 {
            let byte = match value {
                value if value > 0x7F => (value & 0x7F) | 0x80,
                value => value & 0x7F
            };

            stream.write_u8(byte as u8).await?;
            value >>= 7;

            if value == 0 {
                break;
            }
        }

        Ok(())
    }

    async fn write_binary_string(stream: &mut TcpStream, value: String) -> Result<()> {
        let bytes = value.as_bytes();
        RejectCHConnection::write_var_uint(stream, bytes.len() as u64).await?;
        stream.write_all(bytes).await?;
        Ok(())
    }
}
